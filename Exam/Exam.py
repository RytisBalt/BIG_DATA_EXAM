import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import folium
import pandas as pd



hadoop_bin = r"C:\hadoop\bin"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


## PREDEFINED VARIABLES

CENTER_LAT = 55.225000
CENTER_LON = 14.245000
MAX_RADIUS_NM = 50.0
EARTH_RADIUS_NM = 3440.065
BUCKET_DEG   = 0.0018 # About 200 m tolerance
BUCKET_SCALE = 1.0 / BUCKET_DEG


# BUILDING SPARK SESSION AND DEFINING SCHEMA WITH VARIABLES I CHOSE TO BE USEFUL
spark = SparkSession.builder \
    .appName("AIS_Data_Processing") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

my_schema = StructType([
    StructField("# Timestamp", StringType(), True),
    StructField("Type of mobile", StringType(), True),             
    StructField("MMSI", StringType(), True),             
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Navigational status", StringType(), True),
    StructField("ROT", DoubleType(), True),
    StructField("SOG", DoubleType(), True)
])

folder_path = "Large_data/*.csv"

print("Script launched")
start_time = time.time()

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(my_schema) \
    .load("Large_data") \
    .select("# Timestamp", "MMSI", "Latitude", "Longitude", "Navigational status", "SOG")

total_rows = df.count()

# 1.NOISE AND MESSY DATA FILTERING
##################################
cleaned_df = df.filter(
    F.col("MMSI").isNotNull() &
    F.col("Latitude").isNotNull() &
    F.col("Longitude").isNotNull() &
    F.col("SOG").isNotNull() & (F.col("SOG") > 1.5) & 
    F.col("Navigational status").isNotNull() & 
    
    (F.col("Latitude") >= -90.0) & (F.col("Latitude") <= 90.0) &
    (F.col("Longitude") >= -180.0) & (F.col("Longitude") <= 180.0) &
    (F.length(F.col("MMSI").cast("string")) == 9) &
    
    ~F.trim(F.lower(F.col("Navigational status"))).contains("anchor") &
    ~F.trim(F.lower(F.col("Navigational status"))).contains("moored") &
    ~F.trim(F.lower(F.col("Navigational status"))).isin("unknown", "undefined")
).select(
    F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("parsed_time"),
    "MMSI", "Latitude", "Longitude", "SOG" 
)

# 2. RADIUS FILTERING
##################################
vessels_in_radius_df = cleaned_df.withColumn(
    "distance_nm",
    F.acos(
        F.sin(F.radians(F.lit(CENTER_LAT))) * F.sin(F.radians(F.col("Latitude"))) +
        F.cos(F.radians(F.lit(CENTER_LAT))) * F.cos(F.radians(F.col("Latitude"))) *
        F.cos(F.radians(F.col("Longitude")) - F.radians(F.lit(CENTER_LON)))
    ) * F.lit(EARTH_RADIUS_NM)
).filter(F.col("distance_nm") <= MAX_RADIUS_NM)


################
#2. Timestamps and coordinates are converted into integeres in this part
active_window = Window.partitionBy("MMSI")

vessels_with_baseline = vessels_in_radius_df.withColumn(
    "time_seconds", F.unix_timestamp(F.col("parsed_time"))
).withColumn(
    "time_bucket",
    (F.col("time_seconds") / 60).cast("long")
).withColumn(
    "lat_bucket", (F.col("Latitude")  * BUCKET_SCALE).cast("long")
).withColumn(
    "lon_bucket", (F.col("Longitude") * BUCKET_SCALE).cast("long")
).withColumn(
    "Vessel_Avg_SOG",
    F.round(
        F.avg(F.when(F.col("SOG") > 2.0, F.col("SOG"))).over(active_window),
        2
    )
)



##### Takes 10 minute feature window and calculates SOG 10min later and this variable will be used for sorting
future_window = (
    Window.partitionBy("MMSI")
          .orderBy("time_seconds")
          .rangeBetween(0, 600)
)

moving_vessels = vessels_with_baseline.withColumn(
    "SOG_10Min_Later",
    F.max(F.col("SOG")).over(future_window) 
).select(
    "parsed_time", "time_bucket", "time_seconds",
    "lat_bucket", "lon_bucket",
    "MMSI", "Latitude", "Longitude",
    "SOG", "Vessel_Avg_SOG", "SOG_10Min_Later"
)

moving_vessels.show(20)

EARTH_RADIUS_M = 6_371_000
MAX_DIST_M     = 200  # metres 
ship_A = moving_vessels.alias("A")
ship_B = moving_vessels.alias("B")

# CHECKS FOR POTENTIAL COLLIDED SHIPS MMSI check is for more efficient filtering

potential = ship_A.join(
    ship_B,
    (F.col("A.MMSI")        < F.col("B.MMSI"))        &
    (F.col("A.time_bucket") == F.col("B.time_bucket")) &
    (F.col("A.lat_bucket")  == F.col("B.lat_bucket"))  &
    (F.col("A.lon_bucket")  == F.col("B.lon_bucket")),
    how="inner"
)

# Haversine post-filter — precise distance on the small candidate set.
# Eliminates false positives from ships that share a bucket but are
# actually further apart than MAX_DIST_M.

# Also calculates percentage drop in SOG after ,,collision"
potential_collisions_df = potential.withColumn(
    "dlat", F.radians(F.col("B.Latitude") - F.col("A.Latitude"))
).withColumn(
    "dlon", F.radians(F.col("B.Longitude") - F.col("A.Longitude"))
).withColumn(
    "haversine_m",
    F.lit(2 * EARTH_RADIUS_M) * F.asin(F.sqrt(
        F.pow(F.sin(F.col("dlat") / 2), 2) +
        F.cos(F.radians(F.col("A.Latitude"))) *
        F.cos(F.radians(F.col("B.Latitude"))) *
        F.pow(F.sin(F.col("dlon") / 2), 2)
    ))
).filter(
    F.col("haversine_m") <= MAX_DIST_M
).select(
    F.col("A.time_bucket").alias("Collision_Minute"),
    F.col("A.parsed_time").alias("Vessel_A_Time"),
    F.col("B.parsed_time").alias("Vessel_B_Time"),
    F.col("A.MMSI").alias("Vessel_A_MMSI"),
    F.col("B.MMSI").alias("Vessel_B_MMSI"),
    F.col("A.Latitude").alias("Collision_Lat"),
    F.col("A.Longitude").alias("Collision_Lon"),
    F.col("haversine_m").alias("Distance_M"),
    F.col("A.SOG").alias("Vessel_A_Collision_SOG"),
    F.col("B.SOG").alias("Vessel_B_Collision_SOG"),
    F.col("A.Vessel_Avg_SOG").alias("Vessel_A_Normal_Avg_SOG"),
    F.col("B.Vessel_Avg_SOG").alias("Vessel_B_Normal_Avg_SOG"),
    F.col("A.SOG_10Min_Later").alias("Vessel_A_SOG_10Min_Later"),
    F.col("B.SOG_10Min_Later").alias("Vessel_B_SOG_10Min_Later"),
    F.round(
        F.when(F.col("A.Vessel_Avg_SOG") == 0, 0.0)
         .otherwise(
             ((F.col("A.Vessel_Avg_SOG") - F.col("A.SOG")) /
              F.col("A.Vessel_Avg_SOG")) * 100
         ), 1
    ).alias("Vessel_A_Speed_Drop_Percent"),
    F.round(
        F.when(F.col("B.Vessel_Avg_SOG") == 0, 0.0)
         .otherwise(
             ((F.col("B.Vessel_Avg_SOG") - F.col("B.SOG")) /
              F.col("B.Vessel_Avg_SOG")) * 100
         ), 1
    ).alias("Vessel_B_Speed_Drop_Percent"),
)


### FILTERING OUT FLAGGED PAIRS THAT COULD'VE COLLIDED

pair_minute_window = Window.partitionBy("Vessel_A_MMSI", "Vessel_B_MMSI")

flagged_pairs_df = potential_collisions_df.withColumn(
    "Distinct_Minutes_Together", 
    F.size(F.collect_set("Collision_Minute").over(pair_minute_window))
)

filtered_collisions_df = flagged_pairs_df.filter(
    (F.col("Distinct_Minutes_Together") <= 3) &

    (F.col("Vessel_A_Collision_SOG") < 15.0) & 
    (F.col("Vessel_B_Collision_SOG") < 15.0) &
    (F.col("Vessel_A_Normal_Avg_SOG") < 15.0) &
    (F.col("Vessel_B_Normal_Avg_SOG") < 15.0) &

    (
        (F.col("Vessel_A_Speed_Drop_Percent") > 15.0) | 
        (F.col("Vessel_A_Speed_Drop_Percent") < -35.0) | 
        (F.col("Vessel_B_Speed_Drop_Percent") > 15.0) |
        (F.col("Vessel_B_Speed_Drop_Percent") < -35.0) |
        F.col("Vessel_A_SOG_10Min_Later").isNull() | 
        F.col("Vessel_B_SOG_10Min_Later").isNull()
    ) &
    

    ~(
        F.col("Vessel_A_SOG_10Min_Later").isNotNull() &
        (F.col("Vessel_A_SOG_10Min_Later") >= F.col("Vessel_A_Collision_SOG") * 1.10)
    ) &
    ~(
        F.col("Vessel_B_SOG_10Min_Later").isNotNull() &
        (F.col("Vessel_B_SOG_10Min_Later") >= F.col("Vessel_B_Collision_SOG") * 1.10)
    )
).orderBy(
    F.col("Distance_M").asc() # Bring the absolute closest physical encounters to the top
)

print("\n--- CONFIRMED CRITICAL COLLISIONS---")
filtered_collisions_df.show(20, truncate=False)

end_time = time.time()
print(f"Done. {total_rows} rows in {end_time - start_time:.2f}s")


# =====================================================================
# STEP 6: TRAJECTORY VISUALIZATION ENGINE (HARDCODED TARGETS)
# =====================================================================
print("\n[MAP ENGINE] Initializing interactive geographical radar mapping...")


TARGET_A           = 219021240  # Karin Høj
TARGET_B           = 232018267  # Scot Carrier
COLLISION_TIMESTAMP = "2021-12-13 02:27:29"

trajectory_df = df.filter(
    F.col("MMSI").isin(TARGET_A, TARGET_B) &
    (F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss") >= 
        F.to_timestamp(F.lit(COLLISION_TIMESTAMP)) - F.expr("INTERVAL 10 MINUTES")) &
    (F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss") <= 
        F.to_timestamp(F.lit(COLLISION_TIMESTAMP)) + F.expr("INTERVAL 10 MINUTES"))
).select(
    F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("parsed_time"),
    F.col("MMSI").cast("long").alias("MMSI"),
    F.col("Latitude").cast("double").alias("Latitude"),
    F.col("Longitude").cast("double").alias("Longitude"),
    F.col("SOG").cast("double").alias("SOG")
).orderBy("parsed_time")
plot_data = trajectory_df.toPandas()
plot_data["time_str"] = plot_data["parsed_time"].dt.strftime("%H:%M:%S UTC")

ship_A = plot_data[plot_data["MMSI"] == TARGET_A].sort_values("parsed_time").reset_index(drop=True)
ship_B = plot_data[plot_data["MMSI"] == TARGET_B].sort_values("parsed_time").reset_index(drop=True)

print(f"Karin Høj pings   : {len(ship_A)}")
print(f"Scot Carrier pings: {len(ship_B)}")

center_lat = plot_data["Latitude"].mean()  if not plot_data.empty else 55.43
center_lon = plot_data["Longitude"].mean() if not plot_data.empty else 14.55

m = folium.Map(
    location=[center_lat, center_lon],
    zoom_start=14,
    tiles="CartoDB positron"
)

def draw_track(df, color, name):
    if df.empty:
        print(f"No pings for {name}, skipping.")
        return

    n = len(df)

    folium.PolyLine(
        locations=df[["Latitude", "Longitude"]].values.tolist(),
        color=color,
        weight=3,
        opacity=0.7,
        tooltip=name
    ).add_to(m)

    for i, row in df.iterrows():
        opacity = 0.3 + 0.7 * (i / max(n - 1, 1))
        folium.CircleMarker(
            location=[row["Latitude"], row["Longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=opacity,
            tooltip=(
                f"<b>{name}</b><br>"
                f"Time: {row['time_str']}<br>"
                f"SOG: {row['SOG']} kn<br>"
                f"Lat: {row['Latitude']:.5f}<br>"
                f"Lon: {row['Longitude']:.5f}"
            )
        ).add_to(m)

    last = df.iloc[-1]
    folium.Marker(
        location=[last["Latitude"], last["Longitude"]],
        icon=folium.DivIcon(html=f"""
            <div style="
                font-size: 11px;
                font-weight: 600;
                color: {color};
                background: white;
                border: 1px solid {color};
                border-radius: 4px;
                padding: 2px 6px;
                white-space: nowrap;
            ">{name}</div>
        """),
        tooltip=f"{name} — last known position"
    ).add_to(m)

draw_track(ship_A, "#D85A30", "Karin Høj")
draw_track(ship_B, "#378ADD", "Scot Carrier")

collision_dt = pd.Timestamp(COLLISION_TIMESTAMP)
if not plot_data.empty:
    closest_idx = (plot_data["parsed_time"] - collision_dt).abs().argsort().iloc[0]
    closest     = plot_data.iloc[closest_idx]
    impact_lat  = closest["Latitude"]
    impact_lon  = closest["Longitude"]
else:
    impact_lat, impact_lon = center_lat, center_lon

folium.Marker(
    location=[impact_lat, impact_lon],
    popup=folium.Popup(
        f"<b>⚠ Collision</b><br>{COLLISION_TIMESTAMP} UTC<br>"
        f"Lat: {impact_lat:.5f}<br>Lon: {impact_lon:.5f}",
        max_width=220
    ),
    icon=folium.Icon(color="red", icon="exclamation-sign", prefix="glyphicon")
).add_to(m)

output_path = "output/collision_map.html"
m.save(output_path)
print(f"Saved → {output_path}")