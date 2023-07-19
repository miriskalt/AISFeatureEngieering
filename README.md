# AISFeatureEngineering
An pipeline for generating additional AIS features to existing AIS message streams

Open Data Sources of AIS messages used in pipeline:
- [GFW](https://globalfishingwatch.org/data-download/datasets/public-training-data-v1)
- [DMA](https://dma.dk/safety-at-sea/navigational-information/ais-data)

### Authors
Sören Dethlefsen  
Mirjam Bayer  
Tabea Fry  

### Steps performed
1. Data imported into SQL database
2. Deltas of time passed and distance travelled between consequetive AIS massege per vessel
3. Average time computed based on previously computed values
4. Unrealistic values removed based on location and speed. (Thresholds can optionally be adapted when running pipeline)
5. [Water depth](https://globalfishingwatch.org/data-download/datasets/public-bathymetry-v1) added to every AIS message
6. Distance to shore appended if not entailed using [GFW dataset](https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1) or [tif-file](https://doi.org/10.1080/1755876X.2018.1529714)
7. Anchorage detection performed based on distance to shore, stationarity and speed. (Thresholds can optionally be adapted when running pipeline)
8. Full trajectoriesof individual vessels split into trips
9. Data exported as csv containing all additional features

### Further resources
This pipeline is (hopefully) to be presented at the eScience Conference in Limassol, Cyprus in October 9-13, 2023.
The collateral poster paper will contain more details on the features computed in this pipeline. Link will follow if published.
