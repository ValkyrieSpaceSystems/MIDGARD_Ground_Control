"""
peripheral.py — MIDGARD peripheral config loader

File structure:
    Peripherals/
        <manufacturer>/
            <id>.toml           ← e.g. VSS/ASGARD_V0.2.toml
            rocket.toml         ← shared parent configs live here too

Usage:
    peripherals = {
        'rocket': {'interface': 'elrs', 'manufacturer': 'VSS', 'id': 'ASGARD_V0.2'},
    }
    for name, args in peripherals.items():
        peripherals[name] = peripheral(name, args)
"""

import os
import tomllib
from typing import Any
from utility import get_nested


PERIPHERALS_ROOT = os.path.join(os.path.dirname(__file__), "Peripherals")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_toml(manufacturer: str, id_name: str) -> str:
    path = os.path.join(PERIPHERALS_ROOT, manufacturer, f"{id_name}.toml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "rb") as f:
        return tomllib.load(f)


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        elif key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key] = _merge_lists_by_id(result[key], value)
        else:
            result[key] = value
    return result


def _merge_lists_by_id(base: list, override: list) -> list:
    base_map: dict[str, dict] = {
        item["id"]: item.copy() for item in base if "id" in item
    }
    no_id = [item for item in base if "id" not in item]
    for item in override:
        if "id" in item:
            if item["id"] in base_map:
                base_map[item["id"]].update(item)
            else:
                base_map[item["id"]] = item
        else:
            no_id.append(item)
    return list(base_map.values()) + no_id


def _resolve_inheritance(manufacturer: str, id_name: str, _seen: set | None = None) -> dict:
    """
    Recursively resolve `parent` chain and deep-merge configs.
    Parent files are looked up in the same manufacturer folder.
    """
    _seen = _seen or set()
    key = (manufacturer, id_name)
    if key in _seen:
        raise ValueError(f"Circular inheritance: {manufacturer}/{id_name}")
    _seen.add(key)

    raw = _load_toml(manufacturer, id_name)
    parent_id = raw.get("meta", {}).get("parent")

    if not parent_id:
        return raw

    parent_raw = _resolve_inheritance(manufacturer, parent_id, _seen)
    merged = _deep_merge(parent_raw, raw)
    merged.get("meta", {}).pop("parent", None)
    return merged


# ── element classes ──────────────────────────────────────────────────────────


class Interface:
    def __init__(self, entry: dict):
        self.id: str              = entry["id"]
        self.name: str            = entry.get("name", self.id)
        self.description: str     = entry.get("description", "")
        self.type: str            = entry["type"]
        self.protocol: str | None = entry.get("protocol")
        self.baud: int | None     = entry.get("baud")
        # radio fields
        self.freq: int | None     = entry.get("freq")
        self.unit: str | None     = "None"

    def __repr__(self):
        return f"Interface({self.id!r}, type={self.type!r})"


class Phase:
    def __init__(self, entry: dict):
        self.id: str                   = entry["id"]
        self.name: str                 = entry.get("name", self.id)
        self.description: str          = entry.get("description", "")
        self.transition_to: list[str]  = entry.get("transition_to", [])
        self.set_by: list[str]         = entry.get("set_by", [])
        self.unit: str                 = 'str'

    def __repr__(self):
        return f"Phase({self.id!r}, description={self.description!r})"


class Lockout:
    def __init__(self, entry: dict):
        self.id: str                  = entry["id"]
        self.name: str                = entry.get("name", self.id)
        self.description: str         = entry.get("description", "")
        self.default: bool            = entry.get("default", False)
        self.armed_by: list[str]      = entry.get("armed_by", [])
        self.disarmed_by: list[str]   = entry.get("disarmed_by", [])
        self.debug_only: bool         = entry.get("debug_only", False)
        self.unit: str                = 'bool'
        # runtime state
        self.state: bool              = self.default

    def __repr__(self):
        return f"Lockout({self.id!r}, state={self.state})"


class Actuator:
    def __init__(self, entry: dict):
        self.id: str                = entry["id"]
        self.name: str              = entry.get("name", self.id)
        self.description: str       = entry.get("description", "")
        self.type: str              = entry["type"]
        self.subtype: str | None    = entry.get("subtype")
        self.armed_by: list[str]    = entry.get("armed_by", [])
        self.disarmed_by: list[str] = entry.get("disarmed_by", [])
        self.debug_only: bool       = entry.get("debug_only", False)
        # servo fields
        self.pin: int | None                = entry.get("pin")
        self.unit: str | None               = entry.get("unit")
        self.range: tuple | None            = tuple(entry["range"]) if "range" in entry else None
        self.default_position: float | None = entry.get("default_position")
        self.slew_rate_max: float | None    = entry.get("slew_rate_max")
        self.scale: float | None            = entry.get("scale")
        self.offset: float | None           = entry.get("offset")
        # pyro fields
        self.channel: int | None    = entry.get("channel")
        self.channel_a: int | None  = entry.get("channel_a")
        self.channel_b: int | None  = entry.get("channel_b")
        # runtime state
        self.armed: bool            = False
        self.position: float | None = self.default_position    # servos
        self.fired_a: bool          = False                    # pyros
        self.fired_b: bool          = False                    # pyros

    def __repr__(self):
        return f"Actuator({self.id!r}, type={self.type!r}, armed={self.armed})"


class DataStream:
    def __init__(self, entry: dict):
        self.id: str                      = entry["id"]
        self.name: str                    = entry.get("name", self.id)
        self.description: str             = entry.get("description", "")
        self.type: str                    = entry["type"]
        self.subtype: str | None          = entry.get("subtype")
        self.unit: str                    = entry["unit"]
        self.range: tuple                 = tuple(entry["range"])
        self.nominal: tuple               = tuple(entry["nominal"])
        self.component: str | None        = entry.get("component")
        self.sample_rate_hz: float | None = entry.get("sample_rate_hz")
        self.scale: float | None          = entry.get("scale")
        self.offset: float | None         = entry.get("offset")
        # runtime state
        self.value: float | None          = None

    def in_range(self) -> bool:
        if self.value is None:
            return True
        return self.range[0] <= self.value <= self.range[1]

    def in_nominal(self) -> bool:
        if self.value is None:
            return True
        return self.nominal[0] <= self.value <= self.nominal[1]

    def __repr__(self):
        return f"DataStream({self.id!r}, value={self.value} {self.unit})"


# ── Peripheral class ───────────────────────────────────────────────────────────

class Peripheral:
    def __init__(self, stop_event, name: str, args: dict):
        self.name:         str = name
        self.interface_id: str = args["interface"]
        self.manufacturer: str = args["manufacturer"]
        self.id:           str = args["id"]

        raw  = _resolve_inheritance(self.manufacturer, self.id)
        meta = raw.get("meta", {})

        # ── Meta ───────────────────────────────────────────────────────
        self.display_name: str = meta.get("name", self.id)
        self.description: str  = meta.get("description", "")
        self.type: str         = meta.get("type", "unknown")
        self.notes: str        = meta.get("notes", "")
        self.phase: str        = meta.get("first_phase", "")

        # ── element dicts ────────────────────────────────────────────
        self.interfaces:   dict[str, Interface]  = {e["id"]: Interface(e)  for e in raw.get("interface",   [])}
        self.lockouts:     dict[str, Lockout]    = {e["id"]: Lockout(e)    for e in raw.get("lockout",     [])}
        self.phases:       dict[str, Phase]      = {e["id"]: Phase(e)      for e in raw.get("phase",       [])}
        self.actuators:    dict[str, Actuator]   = {e["id"]: Actuator(e)   for e in raw.get("actuator",    [])}
        self.data_streams: dict[str, DataStream] = {e["id"]: DataStream(e) for e in raw.get("data_stream", [])}

        # ── Select active interface ────────────────────────────────────
        self.active_interface: Interface | None = self.interfaces.get(self.interface_id)
        if self.active_interface is None:
            print(f"[MIDGARD WARNING] Interface '{self.interface_id}' not found in {self.id}")

        # ── Validate references ────────────────────────────────────────
        self._validate()

    # ── Validation ─────────────────────────────────────────────────────

    def _validate(self):
        pass
        # check that first phase is a valid phase

    # ── Runtime helpers ────────────────────────────────────────────────


    # idk if these are how i want to do this yet


    def update_data(self, stream_id: str, value: float):
        """Push a new telemetry value to a data stream."""
        ds = self.data_streams.get(stream_id)
        if ds is None:
            print(f"[MIDGARD WARNING] Unknown data stream '{stream_id}'")
            return
        ds.value = value
        #if not ds.in_range():
            #print(f"[MIDGARD ALERT] {stream_id} = {value} {ds.unit} out of range {ds.range}")

    def receive_event(self, event_id: str):
        """
        Mark an event as active when received from the FC.
        Deactivates conflicting events and updates phase accordingly.
        """
        ev = self.events.get(event_id)
        if ev is None:
            print(f"[MIDGARD WARNING] Unknown event '{event_id}'")
            return

        # check prerequisites
        for prereq in ev.prerequisites:
            prereq_id = prereq.removeprefix("event.")
            prereq_ev = self.events.get(prereq_id)
            if prereq_ev and not prereq_ev.active:
                print(f"[MIDGARD WARNING] Event '{event_id}' prerequisite '{prereq}' not met")
                return

        # deactivate conflicts
        for conflict in ev.conflicts:
            conflict_id = conflict.removeprefix("event.")
            conflict_ev = self.events.get(conflict_id)
            if conflict_ev:
                conflict_ev.active = False

        ev.active = True

        # update phases triggered by this event
        for phase in self.phases:
            if f"event.{event_id}" in phase.triggers:
                for s in self.phases:
                    s.active = False
                phase.active = True
                print(f"[MIDGARD] phase → {phase.name}")

    def set_lockout(self, lockout_id: str, state: bool):
        """Set a lockout switch state."""
        lk = self.lockouts.get(lockout_id)
        if lk is None:
            print(f"[MIDGARD WARNING] Unknown lockout '{lockout_id}'")
            return
        lk.state = state
        print(f"[MIDGARD] Lockout '{lockout_id}' → {state}")

    def active_status(self) -> Phase | None:
        for s in self.phases.values():
            if s.active:
                return s
        return None

    def __repr__(self):
        return f"Peripheral({self.name!r}, id={self.id!r})"