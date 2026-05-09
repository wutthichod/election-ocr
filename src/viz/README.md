# Tambon Winner Map

This folder contains a Streamlit + pydeck app that maps the constituency winner in each Lampang tambon using the CSV exports in `data/analysis_exports/`.

## Files

- `app.py`: the Streamlit dashboard.
- `requirements.txt`: minimal dependencies for the viz app.
- `tambon_name_overrides.csv`: optional manual name corrections for OCR-noisy amphoe/tambon names.

## Run

```bash
python -m pip install -r src/viz/requirements.txt
streamlit run src/viz/app.py
```

## Boundary Source

The app fetches Lampang tambon polygons from the public ArcGIS FeatureServer for Thailand subdistrict boundaries and caches the GeoJSON in `src/viz/cache/lampang_tambon_boundaries.geojson` after the first successful run.

Source used in the app:

- ArcGIS Thailand subdistrict boundary layer:
  `https://services1.arcgis.com/jSaRWj2TDlcN1zOC/arcgis/rest/services/Thailand_Subdistrict_Boundaries_(ข้อมูลขอบเขตตำบลประเทศไทย)/FeatureServer/1`

## OCR Name Overrides

If some OCR names do not fuzzy-match correctly, add rows to `src/viz/tambon_name_overrides.csv`:

```csv
raw_amphoe,raw_tambon,official_amphoe,official_tambon
เมืองฯ,ทุ่งฝาย,เมืองลำปาง,ทุ่งฝาย
```

After saving the file, rerun the Streamlit app.
