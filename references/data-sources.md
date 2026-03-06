# Data Sources (Production Matrix)

## Source List (18)

1. USGS — earthquake feed
2. GDACS — disaster alerts
3. NASA EONET — active natural events
4. CoinGecko — crypto pricing
5. Alternative.me — fear & greed index
6. Polymarket — prediction market probabilities
7. NWS — weather alerts (configured point)
8. UCDP — armed conflict events
9. U.S. Treasury — rates proxy
10. USASpending — defense budget references
11. Open-Meteo climate API — anomaly signal
12. GDELT ArtList — conflict/news velocity headlines
13. GDELT Timeline (VolRaw + Tone) — volume/tone regime shifts
14. UNHCR — displacement/refugee statistics
15. Feodo Tracker — C2/botnet infra signal
16. OpenSky — aviation density in conflict zones
17. EIA — oil market prices
18. World Bank — GDP growth enrichment

## Signal Families

- geophysical: quake/disaster/climate/weather
- geopolitical: conflict/news/displacement
- market: crypto/oil/prediction markets
- security: malware C2 + aviation proxy signals
- macro: treasury/worldbank context

## Source Reliability Notes

- GDELT and UCDP can be slow/rate-limited; keep graceful degradation paths.
- TimelineVolRaw/Tone are bursty near breaking events; normalize with `norm` and score via z-sigma windows.
- Prediction and market feeds can spike; enforce thresholding before escalation.
- Weather and regional feeds should be configurable by point/country.

## Tuning Guidance

- use bounded fetch timeouts per source
- cap records per source for deterministic runs
- apply post-fetch sorting/priority filters
- preserve fallback defaults when a source is unavailable
