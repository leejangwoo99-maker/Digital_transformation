# AptivDashboard C# GUI

This is the first WinForms prototype to replace the Streamlit UI.
The current version connects directly to PostgreSQL and does not require FastAPI.

## Location

```text
C:\Users\user\c#_project
```

## Run

```powershell
cd C:\Users\user\c#_project
dotnet run --project .\AptivDashboard.Gui\AptivDashboard.Gui.csproj
```

## Package Install Example

Current chart rendering uses plain WinForms custom drawing, so no chart package is required.
If you later want to add a chart library such as ScottPlot, install it like this:

```powershell
cd C:\Users\user\c#_project
dotnet add .\AptivDashboard.Gui\AptivDashboard.Gui.csproj package ScottPlot.WinForms
```

## Database

- The app reads `DATABASE_URL` from the existing Python `.env`.
- Default lookup paths:
  - `C:\Users\user\PycharmProjects\PythonProject\app\.env`
  - `C:\Users\user\PycharmProjects\PythonProject\.env`
- DB details are hidden in the GUI. Use the `DB 재연결` button if the connection drops.

## Implemented

- Hidden DB connection
- DB reconnect button
- Current-time automatic scope
- Manual production day / shift scope
- 5-second refresh for the selected main tab only
- Four main categories
  - Production status
  - Production info
  - Production analysis
  - Program status
- Email list edit popup
- Barcode edit popup
- Planned time edit popup
- Worker info editable grid
- Insert/update/delete-style save logic for editable tables
- Production STOP placeholder button

## Next Candidates

- Full PDF/email send workflow
- Scheduled 08:30 / 20:30 automatic email send
- More polished chart visuals
