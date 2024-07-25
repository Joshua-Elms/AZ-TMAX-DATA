import requests
import xarray as xr
import io
import numpy as np
from pathlib import Path
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from time import perf_counter

def main(url_func, output_loc, az_cnty_shapefile_loc, year_start, year_stop, month_stop_final):
    output_loc.mkdir(exist_ok=True, parents=True)

    zip_codes = gpd.read_file(az_cnty_shapefile_loc)
    zip_codes = zip_codes[zip_codes["STATE"] == "AZ"]

    gdfs = []
   
    for year in range(year_start, year_stop+1):
        month_stop = 12 if year != year_stop else month_stop_final
        for month in range(1, month_stop+1):
            start = perf_counter()
            url = url_func(year, str(month).zfill(2))
            response = requests.get(url)
            try:
                ds = xr.open_dataset(io.BytesIO(response.content))

            except:
                print(f"Failed to load {year}/{month:02}")
                breakpoint()

            lats, lons = ds.lat.values, ds.lon.values

            # Flatten the latitude and longitude arrays if necessary
            mg_lons, mg_lats = np.meshgrid(lons, lats)
            points = [Point(lon, lat) for lon, lat in zip(mg_lons.flatten(), mg_lats.flatten())]

            for day in range(0, len(ds.time)):

                tmaxs = ds["tmax"].isel(time=day).values.flatten()

                # Create a GeoDataFrame from data points
                points_gdf = gpd.GeoDataFrame(pd.DataFrame({'geometry': points, "tmax": tmaxs}), crs=zip_codes.crs)

                points_with_counties = zip_codes.sjoin(points_gdf, how='left', predicate="dwithin", distance=0.03) # hopefully this is about 2 miles, but who knows w/ projections being what they are
                keep_cols = ["ZIP_CODE", "tmax"]
                gdf = points_with_counties[keep_cols].groupby("ZIP_CODE").mean().reset_index()
                gdf.rename(columns={"ZIP_CODE": "zip_code", "tmax": "daily_Tmax_degF"}, inplace=True)

                gdf["daily_Tmax_degF"] = gdf["daily_Tmax_degF"] * 9/5 + 32 # C to F

                try:
                    gdf["date"] = pd.Timestamp(year=year, month=month, day=day+1)
                except: 
                    print(f"Failed to create date for {year}/{month:02}/{day+1}")
                    breakpoint()

                gdfs.append(gdf)

            elapsed = perf_counter() - start

            print(f"Processed {year}/{month:02} in {elapsed/60:.2f} minutes")

    gdf_all_data = pd.concat(gdfs).reset_index()[["date", "zip_code", "daily_Tmax_degF"]]
    gdf_all_data.index.name = "ID"
    gdf_all_data.to_csv(output_loc / "az_tmax_data.csv")
    breakpoint()

if __name__=="__main__":
    main(
        url_func=lambda year, month: f"https://www.ncei.noaa.gov/data/nclimgrid-daily/access/grids/{year}/ncdd-{year}{month}-grd-scaled.nc",
        output_loc=Path().cwd().resolve() / "data",
        az_cnty_shapefile_loc=None, # insert path to shapefile here
        year_start=2020,
        year_stop=2024,
        month_stop_final=5
    )