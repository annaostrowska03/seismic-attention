# seismic-attention

**Internet as an Earthquake Sensor (USGS + Bluesky + GDELT)** 
Big Data Analytics project, Team MANG.

For every new M4.5+ earthquake in the USGS catalogue, the system measures the Bluesky reaction
(human posts separated from bots) and predicts whether the event will be covered by global media
(GDELT, 48 h), at T0 (USGS features only) and at T+15 min (with social-reaction features).

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pytest
```

## Team

Anna Ostrowska, Gabriela Majstrak, Norbert Frydrysiak, Michał Kukla
