SEND.R4X
========

SEND.R4X ist ein Terminalwerkzeug fuer einfache Netzwerk-Sendetests ueber
R4NET.

Projektstruktur seit 0.51.19:
- `build.zig` baut die App als eigenes SDK-Projekt.
- `build.zig.zon` bindet `r4os_sdk` als Paket.
- `module.R4MF` beschreibt Artefakt, Zielpfad, Imports und Contract.

Build:

    cd Code\System\Software\Send
    ..\..\..\DevTools\Zig\zig.exe build

Ergebnis:

    Code\System\Software\Send\zig-out\SEND.R4X

Contract:
- R4XStart-Entry: `send_main`
- App-Klasse: `console`
- R4L-Imports: `R4SYS`, `R4NET`
- Zielpfad im Image: `C:\R4OS\SOFTWARE\TERMINAL\SEND.R4X`

