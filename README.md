# BIG_DATA_EXAM
## 📋 Prerequisites & Project Setup

Before running the application, you need to ensure your local directory structure is set up correctly. The system processes massive datasets locally, so your data must be placed in a specific folder relative to the execution context.

## Expected Directory Structure

Your project workspace should be structured exactly like this before and after execution:

```
Marine_Project_Workspace/
├── Large_Data/                     # Create this folder manually
│   └── ais_data_12.csv             # Place your raw tracking data here
├── output/                         # Created manually or automatically
│   └── collision_map.html          # ⭐ THE PROCESSED INTERACTIVE MAP LANDS HERE!
├── Dockerfile                      # App container blueprint
├── Exam.py                         # Main PySpark mapping script
└── requirements.txt                # Python dependencies
```

 ## DOCKER HUB IMAGE ##

🔗 **Docker Hub Repository:** [rytisbalt/marine-collision-app](https://hub.docker.com/repository/docker/rytisbalt/marine-collision-app/general)


## OUTPUT ##

Output, which is generated colission map will be stored in the output.

## NOTES ABOUT THE PROJECT ##

During filtering stage, not only the ships that actually collided were retrieved, but those which collided were also part of the set. My guess would be that a lot rescue boats were coming around that location and that made the task harder. 

Map is generated accurately and we can see that the smaller boat was hit by the bigger boat, looking at the impact.

## AI USAGE ##

AI was used to consult, regarding map generation and efficient scaling logic. 






