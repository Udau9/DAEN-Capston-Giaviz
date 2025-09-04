# Real-Time Situational Awareness (RTSA)

**Team Name:** Data Fusion  
**Project Type:** Capstone – Real-Time 3D Data Fusion and Visualization

---

## 🌍 Project Overview
This project aims to fuse disparate data sources to provide a unified, real-time view of situations, helping decision-makers respond faster and more effectively during crises.  

We are building a system that transforms overwhelming amounts of raw data into **clear, actionable insights** that can save time, resources, and lives.

---

## 🎯 Mission & Goals
Our mission is to empower emergency responders, city planners, and authorities by:

- **Integrating real-time and historical data** from the Maryland, D.C., and Virginia (DMV) region with <10 min latency.  
- **Applying machine learning and computer vision** to automatically detect hazards, predict route blockages, and suggest alternate paths.  
- **Visualizing insights in a 3D environment** (via GaiaViz) so complex information becomes intuitive and actionable during high-pressure situations.  

💡 **Example Scenario:**  
Imagine a wildfire blocking a major highway. Our platform could instantly reroute fire engines, coordinate with utility crews to assess power grid damage, and suggest safe evacuation paths for citizens.

---

## 🔧 Project Architecture
The project will be executed in four phases:

1. **Data Collection & Integration**  
   Gather real-time feeds (traffic incidents, weather sensors, flight tracking) and non-real-time datasets (infrastructure maps, outage archives, disaster history).

2. **Data Processing & Fusion**  
   Clean, normalize, and unify spatial, temporal, and contextual information into a comprehensive data model.

3. **Machine Learning & Computer Vision**  
   Train models to detect incidents from video, predict alternate emergency routes, and forecast impacts on infrastructure.

4. **Interactive 3D Visualization**  
   Use **GaiaViz** to overlay incidents, infrastructure, and predictive analytics in a single intuitive 3D interface.

---

## 📊 Key Data Sources
- **Real-time (<10 min latency):**  
 VDOT SmarterRoads data, weather APIs, ADS-B flight tracking.  

- **Non-real-time (historical/static):**  
  GIS infrastructure maps, power outage archives, historical disaster datasets.  

---

## 🚀 Current Status
We are in **Phase 1: Dataset Discovery and Scoping**.  
**Next steps:**
- Finalize data sources for DMV region  
- Build ingestion and cleaning pipelines  
- Prototype data fusion and visualization workflows  
- Experiment with computer vision on live traffic feeds  

---

## 📚 References & Inspiration
- H. Xu et al., *Smart Mobility in the Cloud: Enabling Real-Time Situational Awareness and Cyber-Physical Control Through a Digital Twin for Traffic*, IEEE Access, 2022.  
- D. Correa & J. C. Falcocchio, *A Data-Driven Case Study Following the Implementation of an Adaptive Traffic Control System in Midtown Manhattan*, JTE, 2022.  
- M. Bouabid & M. Farah, *GAZADeepDav: A High Resolution Geotagged Satellite Imagery Dataset For Analyzing War-Induced Damage*, IGARSS, 2024.  
- M. Chmielewski, *Data Fusion Based on Ontology Model for Common Operational Picture Using OpenMap and Jena Semantic Framework*, Military University of Technology.  

---

## 👥 Team Roles – *Data Fusion*
- **Sushanth Reddy Rachala** – Scrum Master  
  Guides team in Agile practices, facilitates meetings, and removes blockers.  
- **Madhu Yamini Nainala** – Product Owner  
  Defines product vision, manages backlog, and prioritizes features.  
- **Sri Ram Jawahar Vennela** – Developer  
  Designs, codes, tests, and maintains software.  
- **Uday Sai Shankar Kola** – Developer  
  Contributes to core implementation and technical tasks.  
- **Chrissie Raj Benzam** – Developer  
  Focuses on building and integrating project features.  

---

## 📌 License
- **Project code & deliverables:** Licensed under [Apache License 2.0](LICENSE).  
- **GaiaViz desktop application:** Licensed under [CC BY-ND 4.0](https://creativecommons.org/licenses/by-nd/4.0/).  
  > *Note: GaiaViz cannot be modified or redistributed in derivative form; we integrate it as-is.*  

---

## 🏷️ Badges (Optional)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)  
[![GaiaViz: CC BY-ND 4.0](https://img.shields.io/badge/GaiaViz-CC%20BY--ND%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nd/4.0/)  
