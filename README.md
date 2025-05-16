# Big Data Project - ClimAlytics AT


## Team Members
- **David Kalteis - s2410455001**  
- **Dominik Forsthuber - s2410455011**  
- **Michael Kerscher - s2410455014**

---

## Dataset Overview
| **Title**            | Messstationen Monatsdaten v2                 |
|----------------------|-----------------------------------------------|
| **Provider**         | [GeoSphere Austria](https://www.geosphere.at/de) |
| **Agency**           | Bundesanstalt für Geologie, Geophysik, Klimatologie und Meteorologie |
| **Download Portal**  | [API Frontend](https://dataset.api.hub.geosphere.at/app/frontend/station/historical/klima-v2-1m) |
| **Stored**         | [FH OOE Sharepoint](https://fhooe-my.sharepoint.com/:f:/r/personal/s2410455014_fhooe_at/Documents/BIG%20Data%20Project%20-%20ClimAlytics%20AT?csf=1&web=1&e=UXfiyS) |

This dataset contains monthly aggregated climate data from various weather stations across Austria. Each entry includes numerous meteorological measurements (e.g., temperature extremes, precipitation, humidity, sunshine duration, frost days, wind data) over many years, making it suitable for large-scale time series and spatial weather analysis.

Time span: January 1970 – April 2025 (monthly resolution)

---

## Project Idea
Our project focuses on analyzing long-term weather trends and extreme climate patterns in Austria using scalable Big Data technologies. We aim to uncover patterns in temperature, precipitation, and extreme events across regions and over time.

**Master’s Thesis Integration:**
- **Drone Operations Thesis:** Analyze wind profiles, turbulence proxies, visibility windows, and temperature extremes to optimize safe-flight scheduling and sensor calibration for firefighting drones.
- **Installer Support App Thesis:** Develop climate-driven work-order recommendations by mapping frost/heat risk periods and daylight availability for field technicians, ensuring safety and efficiency in service operations.

---

## Technology Stack
- **Apache Spark** – In-memory distributed data processing  
- **Apache Parquet** – Columnar storage format for fast, splittable I/O  
- **PyArrow** – Efficient conversion between Parquet and in-memory tables  

<div style="page-break-after: always;"></div>

## Research Questions and Master Thesis Support

1. **Altitude-Dependent Warming**  
   _How does the long-term trend in mean monthly temperature vary with elevation?_  
   - **Drone Thesis:**  
     - Characterize warming rates by elevation to optimize thermal-camera calibrations and flight-season scheduling in mountainous fire zones.  
   - **Installer App Thesis:**  
     - Tailor heat/freeze-risk alerts in the app based on local altitude trends (e.g. valley vs. rooftop installations).

2. **Spatial Patterns of Extremes**  
   _Which geographic zones (valleys, plateaus, alpine corridors) show the largest shifts in “hot days” (≥ 30 °C) and “frost days” (≤ 0 °C) since 1970?_  
   - **Drone Thesis:**  
     - Identify emerging heat/frost corridors to plan safe drone routes and manage battery performance under extreme temperatures.  
   - **Installer App Thesis:**  
     - Drive geofenced notifications for technicians entering areas with rising freeze-thaw or heat-stress events.

3. **Data-Coverage Profiling**  
   _How do station installation dates and validity periods create spatio-temporal gaps, and where are the largest “data deserts”?_  
   - **Both Theses:**  
     - Flag low-confidence zones in both drone-mission planning and installer-app forecasts where metadata shows sparse or intermittent records.  
     - Recommend temporary sensor deployments for critical seasons in high-impact regions.

4. **Operational Window Optimization**  
   _Which seasonal windows and locations optimize safety—combining sunshine hours, wind-gust flags, and frost/heat indicators?_  
   - **Drone Thesis:**  
     - Generate region-specific “safe-flight” calendars for fire-response missions using combined climate metrics.  
   - **Installer App Thesis:**  
     - Power daily work-order advice (e.g. “no frost” + “sufficient daylight” for rooftop tasks, “low wind” for elevated work).

<div style="page-break-after: always;"></div>

# Appendix: Project Protocol and Key Impacts

This protocol outlines our engagement with GeoSphere Austria’s Klima-V2 1M API and highlights key technical steps, API limits, and the impacts of our approach, without excessive detail.

## 1. Metadata Discovery
- Queried the metadata endpoint  
  ```text
  GET /v1/station/historical/klima-v2-1m/metadata?format=JSON

Retrieved:
- **1 134** valid station IDs  
- **420** parameter codes  

## 2. API Limits
- **Rate limit:** 240 requests/hour (≈5 requests/second)  
- **Size limit:** maximum 1 000 000 data values per request  
- **Endpoint structure:**  
GET /v1/station/historical/klima-v2-1m/{station_id} ?parameters=<comma-separated codes> &start=<YYYY-MM-DD> &end=<YYYY-MM-DD> &format=JSON

## 3. Implementation Summary
- **Metadata call:** Built lists of station IDs and parameter names.  
- **Main loop (per station):**  
1. Send a single `GET` request with the station in the path.  
2. Extract `features[0].properties.parameters`, each containing a monthly `data` array.  
3. Iterate through months, writing rows of the form:  
   ```
   station_id, YYYY-MM, <param1>, <param2>, …, <param420>
   ```  
4. Wait **15 seconds** between requests to honor rate limits.

## 4. Overall Impact 
- Ensured **completeness** of all 1 134 × ~660 records × 420 parameters.
- The specially generated climate_all_stations.csv file is the main data set.
- The metadata csv files for parameter and stations from the api hub of GeoSphere Austria and support the project 
- Python code files are attached.
