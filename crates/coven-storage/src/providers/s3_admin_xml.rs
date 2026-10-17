//! Validated XML trees for IAM response envelopes and listing records.

use super::unescape;
use crate::{StorageError, StorageFailure};

pub(crate) struct Element {
    pub(crate) name: String,
    text: String,
    pub(crate) children: Vec<Element>,
}

fn malformed() -> StorageError {
    StorageFailure::Protocol.with_source("malformed IAM XML response")
}

impl Element {
    pub(crate) fn child(&self, name: &str) -> Result<&Self, StorageError> {
        self.optional_child(name)?.ok_or_else(malformed)
    }

    pub(crate) fn optional_child(&self, name: &str) -> Result<Option<&Self>, StorageError> {
        if !self.text.trim().is_empty() {
            return Err(malformed());
        }
        let mut children = self.children.iter().filter(|child| child.name == name);
        let child = children.next();
        if children.next().is_some() {
            return Err(malformed());
        }
        Ok(child)
    }

    pub(crate) fn value(&self) -> Result<&str, StorageError> {
        if self.text.trim().is_empty() || !self.children.is_empty() {
            return Err(malformed());
        }
        Ok(&self.text)
    }

    pub(crate) fn members(&self) -> Result<&[Self], StorageError> {
        if !self.text.trim().is_empty() {
            return Err(malformed());
        }
        Ok(&self.children)
    }

    pub(crate) fn elements(&self, name: &str, found: &mut Vec<String>) {
        if self.name == name {
            found.push(self.text.clone());
        }
        for child in &self.children {
            child.elements(name, found);
        }
    }
}

/// An empty response has no root; listings require one at their boundary.
pub(crate) fn parse(xml: &str) -> Result<Option<Element>, StorageError> {
    let mut root = None;
    let mut stack: Vec<(String, Element)> = Vec::new();
    for token in xmlparser::Tokenizer::from(xml) {
        let token = token.map_err(|error| StorageFailure::Protocol.with_source(error))?;
        match token {
            xmlparser::Token::ElementStart { prefix, local, .. } => {
                if stack.is_empty() && root.is_some() {
                    return Err(malformed());
                }
                stack.push((
                    format!("{prefix}:{local}"),
                    Element {
                        name: local.as_str().into(),
                        text: String::new(),
                        children: Vec::new(),
                    },
                ));
            }
            xmlparser::Token::Text { text } => {
                let text = unescape(text.as_str())?;
                if let Some((_, parent)) = stack.last_mut() {
                    parent.text.push_str(&text);
                } else if !text.trim().is_empty() {
                    return Err(malformed());
                }
            }
            xmlparser::Token::Cdata { text, .. } => {
                stack
                    .last_mut()
                    .ok_or_else(malformed)?
                    .1
                    .text
                    .push_str(text.as_str());
            }
            xmlparser::Token::ElementEnd { end, .. } => {
                if end == xmlparser::ElementEnd::Open {
                    continue;
                }
                let (name, element) = stack.pop().ok_or_else(malformed)?;
                if let xmlparser::ElementEnd::Close(prefix, local) = end {
                    if name != format!("{prefix}:{local}") {
                        return Err(malformed());
                    }
                }
                if let Some((_, parent)) = stack.last_mut() {
                    parent.children.push(element);
                } else {
                    root = Some(element);
                }
            }
            xmlparser::Token::Attribute { .. }
            | xmlparser::Token::Declaration { .. }
            | xmlparser::Token::Comment { .. }
            | xmlparser::Token::ProcessingInstruction { .. } => (),
            _ => return Err(malformed()),
        }
    }
    if !stack.is_empty() {
        return Err(malformed());
    }
    Ok(root)
}
