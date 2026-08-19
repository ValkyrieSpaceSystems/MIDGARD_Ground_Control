# MIDGARD Ground Station

An open-source ground station software system built for flexibility, modularity, and expandability. MIDGARD was originally designed to communicate with the [ASGARD flight computer](https://github.com/ValkyrieSpaceSystems/ASGARD_Flight_Computer) over the [BIFROST protocol](https://github.com/ValkyrieSpaceSystems/BIFROST_Communications_Protocol), but can communicate with any remote system that speaks BIFROST. It's built primarily for high power rocketry, but is general enough to run drones, planes, rovers, or other remote/autonomous systems.

A Python backend owns all communication, safety logic, and command authority. A browser-based frontend, built on NASA's Open MCT framework, handles visualization and control. The backend owns every switch, readout, and actuator state; the frontend displays that state and can request changes, but never sets it directly.

---

## Configuration

Every peripheral is defined through TOML files rather than hardcoded into the software. A config file describes a peripheral's data streams, switches, actuators, and safety lockouts, and can inherit from other TOML files to share common properties across similar hardware. Adding, removing, or reconfiguring a peripheral is a matter of editing data files, not source code.

---

## Safety Philosophy

MIDGARD is designed to maximize safety around the rocket and pad. Key safety features include:

- **Physical confirmation to actuate** — actuating a switch requires holding a confirmation key (default is shift) at the moment of the action, preventing accidental single-click actuation.
- **Backend-confirmed switch states** — a switch change is only a request and Python evaluates it and must confirm the new state before the GUI reflects it.
- **Configurable lockouts** — safety interlocks and conditions are defined per peripheral in TOML, not hardcoded.
- **Robust error handling** — the system is built to fail predictably and safely instead of catastrophically and unpredictably.
- **Fail-safe topology** — the system defaults to the safest state when something goes wrong (communication loss, unexpected error, etc.) rather than an undefined or last-known state.

---

## Communications

MIDGARD only uses BIFROST when a peripheral supports it. When it does, the physical interface in between (ELRS, XBee, RS-485, CAN, USB, etc.) repackages BIFROST for transmission, and the receiving side decodes it back into BIFROST, the protocol itself doesn't change no matter what's carrying it.

For systems that don't speak BIFROST, such as NI or LabJack hardware, MIDGARD interprets their native data format directly in Python and logs it accordingly.

---

## Interface

The GUI is built on Open MCT. MIDGARD automatically generates the JS files that register each peripheral's data sources and actuators with Open MCT, based on the same TOML configs that define the hardware, so no manual configuration is required to add a peripheral. Arranging that data into layouts, panels, and views is left to the user, using Open MCT's Import/Export plugin to save a finished layout as JSON. 

---

## Data Handling

All telemetry and commands are saved to file by a dedicated logging function. Data is also streamed to the GUI in real time and available historically for post-flight review. 