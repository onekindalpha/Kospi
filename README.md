# Kospi

KOSPI breadth data repository based on the market breadth interpretation style from  
**Stan Weinstein, _Secrets for Profiting in Bull and Bear Markets_**  

## 🚀 Live Demo
[https://xkpzgn64fbayeu4wtgaa28.streamlit.app](https://xkpzgn64fbayeu4wtgaa28.streamlit.app)

## Purpose
This repository stores daily KOSPI breadth data used to analyze:

- Advance-Decline breadth
- A/D Line
- Breadth thrust
- Price vs breadth confirmation / divergence

The interpretation is inspired by Stan Weinstein's book-style market reading approach,  
especially the idea of comparing index movement with the internal market breadth.

## Files
- `kospi_breadth.csv`: daily KOSPI advance-decline breadth data

## Columns
- `date`
- `advances`
- `declines`
- `unchanged`
- `ad_diff`
- `ad_line`
- `breadth_thrust_ema10`

## Notes
- `ad_diff = advances - declines`
- `ad_line` is the cumulative advance-decline line
- This repository is for breadth tracking and book-style market interpretation
