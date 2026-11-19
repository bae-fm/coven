/-! Two CTEs of one name (`derived::Ctes`): a WITH inside a subquery, a CTE
or a view can declare a name that an outer WITH already declares. SQLite
resolves the name a FROM item writes to the innermost CTE of that name
visible where the item is, and a CTE's own query names what is visible where
the CTE is declared: the CTEs of its own WITH and of the WITHs around it,
never those of a WITH inside the query that reads it. The runner reads a
statement's tree by this rule (`Json.readItem`), so each FROM item that names
a CTE reads the query SQLite reads there, and the differential compares the
rows the model gives with SQLite's. -/
namespace SqliveNarrowing.CteNames

/-- The CTEs visible where a FROM item is: each WITH's CTEs, by name, the
WITHs around it before those inside them. -/
abbrev Visible (α : Type) := List (List (String × α))

/-- The CTE that `name` names where `visible` is: the query of the innermost
CTE of that name, and the CTEs that query can name, the WITHs from the
outermost to the CTE's own. -/
def resolve {α : Type} : Visible α → String → Option (α × Visible α)
  | [], _ => none
  | ctes :: inner, name =>
    match resolve inner name with
    | some (query, scope) => some (query, ctes :: scope)
    | none => (ctes.find? (·.1 == name)).map fun (_, query) => (query, [ctes])

/-- A name an inner WITH declares names the inner CTE there, whatever the
WITHs around it declare, and its query names the CTEs of that WITH and of
the WITHs around it. -/
theorem resolve_inner {α : Type} (outer : Visible α) (ctes : List (String × α)) (name : String)
    {declared : String} {query : α} (h : ctes.find? (·.1 == name) = some (declared, query)) :
    resolve (outer ++ [ctes]) name = some (query, outer ++ [ctes]) := by
  induction outer with
  | nil => simp [resolve, h]
  | cons around rest ih => simp [resolve, ih]

/-- A WITH that doesn't declare a name changes nothing of what the name
names inside it: the same CTE, whose query names the same CTEs, so an outer
CTE's query names the outer CTEs even where an inner WITH declares one of
their names. -/
theorem resolve_outer {α : Type} (outer : Visible α) (ctes : List (String × α)) (name : String)
    (h : ctes.find? (·.1 == name) = none) :
    resolve (outer ++ [ctes]) name = resolve outer name := by
  induction outer with
  | nil => simp [resolve, h]
  | cons around rest ih => simp [resolve, ih]

end SqliveNarrowing.CteNames
