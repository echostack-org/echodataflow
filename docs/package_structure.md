# Package structure

## Prefect terminology
- flow
- task
- deployment
- server
- worker
- work pool
- anything else that i missed?




## Package structure
- list the folder structure and explain succinctly what each one contains
- for the tasks folder, make sure to explain that this abstraction layer (not sure if this is the right way to describe it) make it possible to use different task runners, and also allows combining different operations within a single task, and that flows can combine different tasks in a single flow
- talk about how tests are organized too




## Available flows
- include only a subset of the flows -- those used by the hake edge-to-cloud pipeline
- these are in (list them based on alphebatical order, not like the random list below):
    - flows_acoustics (non-postprocessing ones)
    - flows_biology
    - flows_classify_backscatter (non-postprocessing ones)
    - flows_helper
    - flows_integration
    - flows_viz_cloud
    - flows_simulation
