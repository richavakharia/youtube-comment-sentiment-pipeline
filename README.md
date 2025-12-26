# YouTube Comment Sentiment & Engagement Pipeline

## Overview
This project builds an end-to-end data pipeline to analyze YouTube comment sections and surface engagement and sentiment signals at scale. Using a combination of Unix-based data processing and PySpark, the pipeline transforms raw comment data into frequency summaries, structural insights, and lightweight sentiment indicators.

The goal is to treat YouTube comments as a form of crowdsourced feedback and answer a practical question:
**Is this video worth watching?**

---

## Project Context
YouTube is a major learning and information platform, but users often face too many videos and too little time. By analyzing comment activity, keyword patterns, and engagement structure, this project explores how large-scale comment data can help identify high-value and high-engagement content.

This repository contains a curated, portfolio-focused version of a semester-long big data project completed as part of a university Big Data course.

---

## Dataset
Data was collected using the **YouTube Data API**, focusing on comments, videos, and channels.

- ~300K raw comments collected
- ~190K cleaned and valid comments
- ~2,000 videos
- ~150K distinct authors

Each comment record includes fields such as:
- `video_id`
- `comment_id`
- `author_display_name`
- `published_at`
- `like_count`
- `comment_text`
- `is_reply`
- `parent_id`
- `channel_id`

Only small samples are included in this repository for reproducibility and clarity. Full datasets are intentionally excluded.

---

## What This Pipeline Does
At a high level, the pipeline:
- Cleans and normalizes raw CSV comment data
- Generates frequency tables and Top-N summaries
- Builds “skinny tables” for compact analysis
- Applies quality filters and engagement buckets
- Performs temporal aggregation (month-level activity)
- Identifies keyword-based sentiment and signal patterns
- Supports cluster and structural analysis from prior stages
- Transitions core analyses to PySpark for distributed execution

The pipeline is reproducible end-to-end using a single entry script.

---

## Technologies Used
- Bash / Unix shell tools (sed, awk, sort, uniq, grep)
- Python
- PySpark / Spark SQL
- Google Colab / cloud-based Spark environment
- CSV / TSV data formats

---

## How to Run
The main entry point is:

```bash
run_pa4.sh
