import CovenIO.ListingFinality
import Lean

/-! Exercise observationTime and the provider transitions of through_time.
The returned complete endpoint is compared with the input to Rust finality. -/
open Lean CovenIO

private def run (j : Json) : Except String Json := do
  let time ← j.getObjValAs? Nat "clock"
  let unit ← j.getObjValAs? Nat "unit"
  let stored ← j.getObjValAs? (Array Nat) "stored"
  if unit = 0 then throw "provider unit must be positive"
  let mut provider : Provider := ⟨empty, 0⟩
  for i in List.range stored.size do
    if stored[i]! < time then
      provider := (serve { provider with time := stored[i]! } []
        (.create (.log .entry i 1) [i])).1
  let clock := { provider with time := time }
  provider := clock
  for i in List.range stored.size do
    if time ≤ stored[i]! then
      provider := (serve { provider with time := stored[i]! } []
        (.create (.log .entry i 1) [i])).1
  let through := if listingMargin + unit ≤ time then some (observationTime time unit) else none
  let certified := stored.map fun t => through.any (fun endpoint => t ≤ endpoint)
  let agrees := (List.range stored.size).all fun i =>
    !certified[i]! || decide (clock.objects (.log .entry i 1) = provider.objects (.log .entry i 1))
  let mut output := [("horizon_input", toJson through),
    ("certified", toJson certified), ("through_time", toJson agrees)]
  if (← j.getObj?).contains "past" then
    let past ← j.getObjValAs? (Array (List Nat)) "past"
    let received ← j.getObjValAs? (List Nat) "received"
    let window ← j.getObjValAs? Nat "window"
    if past.size != stored.size then throw "one recorded past per entry required"
    let H : CovenStorelog.Finality.History :=
      ⟨fun e => ⟨0, e, past[e]!, if e = 0 then .create "root" else .addDevice 0 e⟩,
        fun e => stored[e]!, fun e => stored[e]!⟩
    let receipts := CovenStorelog.entrySet received
    let complete := through.map fun endpoint =>
      (List.range stored.size).all fun e => !(stored[e]! ≤ endpoint) || receipts e
    let observed := through.map fun endpoint =>
      ListingFinality.observedQuiet H window stored.size endpoint receipts
    let whole := through.map fun endpoint =>
      CovenStorelog.Horizon.quiet H window stored.size endpoint
    let recovered := through.bind fun endpoint =>
      if complete = some true then
        some (CovenStorelog.Horizon.recovered H window stored.size endpoint)
      else none
    output := output ++ [("completed_prefix", toJson complete),
      ("observed_quiet", toJson observed), ("whole_quiet", toJson whole),
      ("horizon", toJson recovered)]
  return Json.mkObj output

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let parsed ← Json.parse input
    let samples ← parsed.getArr?
    toJson <$> samples.mapM run
  match result with
  | .ok output => (← IO.getStdout).putStrLn output.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
