# Project Instructions

## Greenfield Development

coven is greenfield software. Implement the intended design directly. Do not
add or retain backward compatibility, compatibility shims or branches, legacy
formats or readers and writers, fallback paths, or migrations for earlier coven
development states unless the user explicitly requests them.

Delete superseded paths and update every caller, test, fixture, and document to
the single current shape. The application-facing schema migration system is a
product capability; it does not authorize preserving obsolete coven internals.
