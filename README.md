# E-Commerce ELT Pipeline

Ein End-to-End-Data-Engineering-Projekt zur Verarbeitung und Analyse von E-Commerce-Bestellungen.

Die Pipeline ruft Bestelldaten über eine REST-API ab, speichert Rohdaten im Objektspeicher MinIO, lädt sie in ein analytisches Data Warehouse mit DuckDB und transformiert sie mit dbt. Dagster orchestriert den Workflow, während ein Streamlit-Dashboard die Analyse der Lieferperformance ermöglicht.

Das Projekt demonstriert zentrale Data-Engineering-Konzepte anhand einer lokal ausführbaren Pipeline.

## Projektziele

- Aufbau einer modularen ELT-Pipeline
- Zuverlässige Datenextraktion mit Checkpoints und Wiederholungsversuchen
- Speicherung von Rohdaten im Parquet-Format
- Transformation und Modellierung mit SQL und dbt
- Automatisierte Datenqualitätsprüfungen
- Orchestrierung mit Dagster
- Visualisierung von Kennzahlen mit Streamlit
- Containerisierung der benötigten Services und automatisierte CI-Prüfungen

## Architektur im Überblick

```text
REST-API (FastAPI)
       |
       v
Python-Extraktion
Checkpoints, Retries,
Schemaänderungen prüfen
       |
       v
MinIO
Rohdaten im Parquet-Format
       |
       v
DuckDB
Raw Layer
       |
       v
dbt
Staging -> Intermediate -> Marts
       |
       v
Dagster
Orchestrierung des Workflows
       |
       v
Streamlit
Dashboard zur Lieferperformance
```

Eine ausführlichere Beschreibung findest du in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Technologie-Stack

| Technologie | Einsatz im Projekt |
|---|---|
| Python | Datenextraktion und Pipeline-Logik |
| FastAPI | Bereitstellung der simulierten REST-API |
| MinIO | Speicherung der Rohdaten als Objekte |
| Parquet | Spaltenorientiertes Dateiformat für Rohdaten |
| DuckDB | Analytisches Data Warehouse |
| SQL und dbt | Transformationen, Datenmodellierung und Tests |
| Dagster | Orchestrierung und Überwachung der Pipeline |
| Streamlit | Interaktives Dashboard |
| Docker Compose | Lokaler Betrieb der Services |
| GitHub Actions | Automatisierte CI-Prüfungen |

## Zentrale Funktionen

### Datenextraktion

- Abruf von Bestellungen über eine REST-API
- Checkpoints zur Fortsetzung der Extraktion
- Wiederholungsversuche bei vorübergehenden Fehlern und Rate Limits
- Prüfung auf Veränderungen des API-Schemas

### Speicherung und Data Warehouse

- Ablage der extrahierten Rohdaten als Parquet-Dateien in MinIO
- Laden der Daten in DuckDB
- Trennung zwischen Rohdaten und transformierten Datenmodellen

### Transformation und Datenqualität

Die dbt-Modelle sind in drei Schichten organisiert:

1. **Staging:** Aufbereitung und Vereinheitlichung der Quelldaten
2. **Intermediate:** Ableitung von Merkmalen zur Lieferverzögerung
3. **Marts:** Aggregierte Kennzahlen für die Analyse der Lieferperformance

Datenqualitätstests prüfen unter anderem Pflichtfelder, eindeutige Bestell-IDs, erlaubte Werte und Bestelldaten in der Zukunft.

### Orchestrierung und Visualisierung

Dagster steuert die Pipeline über einen zentralen Workflow. Das Streamlit-Dashboard stellt ausgewählte Kennzahlen zur Lieferperformance bereit.

## Testergebnisse

Die Pipeline wurde in der lokalen Entwicklungsumgebung erfolgreich ausgeführt.

- **Dagster:** erfolgreicher Lauf des Jobs `full_elt_pipeline`
- **dbt:** 9 erfolgreiche Datentests
- **Rohdaten:** 500 Bestellungen
- **Staging-Modell:** 500 Bestellungen
- **Eindeutige Bestell-IDs:** 500 in beiden Schichten
- **Kennzahlenabgleich:** Die Anzahl der analysierten Bestellungen und der verspäteten Lieferungen stimmte zwischen Intermediate-Modell und Mart überein.

Die Ergebnisse beziehen sich auf den getesteten lokalen Datenbestand. Sie stellen keinen Nachweis für einen produktiven Betrieb dar.

## Projekt lokal ausführen

### Voraussetzungen

- Git
- Docker Desktop mit Docker Compose
- Python in einer zur Projektkonfiguration passenden Version
- Installierte Projektabhängigkeiten

### 1. Repository klonen

```bash
git clone https://github.com/riodeb21195/ecom-elt-pipeline.git
cd ecom-elt-pipeline
```

### 2. Umgebungsvariablen konfigurieren

```bash
cp .env.example .env
```

Prüfe die benötigten Umgebungsvariablen in `.env.example` und passe die lokale Konfiguration bei Bedarf an. Zugangsdaten gehören nicht in das öffentliche Repository.

### 3. Services starten

```bash
docker compose up -d
docker compose ps
```

Prüfe, ob die benötigten Container laufen. Die simulierte API lässt sich bei laufendem Service beispielsweise mit folgendem Befehl prüfen:

```bash
curl http://localhost:8000/health
```

### 4. Python-Abhängigkeiten installieren

Richte die Python-Umgebung entsprechend der Projektkonfiguration ein und installiere die benötigten Abhängigkeiten aus der vorgesehenen Dependency-Datei.

### 5. Dagster starten

Führe den Befehl im Projektstammverzeichnis aus:

```bash
dagster dev -f orchestration/dagster_pipeline/definitions.py
```

Öffne anschließend [http://localhost:3000](http://localhost:3000) und starte den Job `full_elt_pipeline`.

### 6. Dashboard starten

Öffne ein separates Terminal im Projektverzeichnis:

```bash
streamlit run dashboard/app.py
```

Das Dashboard ist anschließend normalerweise unter [http://localhost:8501](http://localhost:8501) erreichbar.

### 7. Datenqualität und Unit-Tests prüfen

Führe dbt-Befehle im passenden dbt-Projektverzeichnis und mit der vorgesehenen Profilkonfiguration aus:

```bash
dbt build
```

Die Python-Unit-Tests werden vom Projektstammverzeichnis aus gestartet:

```bash
pytest tests/unit -v
```

**Hinweis:** Die Schritte beschreiben den vorgesehenen lokalen Ablauf. Vor einer vollständigen Reproduzierbarkeitszusage müssen insbesondere Abhängigkeiten, dbt-Profil, Umgebungsvariablen und der gesamte Startprozess auf einer frischen Umgebung geprüft werden.

## Projektstruktur

```text
ecom-elt-pipeline/
├── README.md
├── ARCHITECTURE.md
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── mock_source_api/
├── extract/
├── staging_minio/
├── warehouse/
├── dbt_project/
├── orchestration/
├── dashboard/
├── tests/
└── .github/workflows/
```

Die technische Aufteilung der Komponenten wird in der [Architekturdokumentation](ARCHITECTURE.md) erläutert.

## Projektstatus

Portfolio-Projekt mit Fokus auf Datenextraktion, ELT, analytische Datenmodellierung, Datenqualität und Workflow-Orchestrierung.

**Repository:** [riodeb21195/ecom-elt-pipeline](https://github.com/riodeb21195/ecom-elt-pipeline)