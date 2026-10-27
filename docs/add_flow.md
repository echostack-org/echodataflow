# How to add a flow

- one needs to add a flow and include it in the paired recipes to run it using the orchestration mechanisms provided by Echodataflow. 
- below we explain the steps to construct a flow and configure a recipe to run it


## Writing a minimum flow
- in general, a flow is a python function decorated with `@flow`, and a task is a python function decorated with `@task`
- the flow can contain just a series of computational steps that one would like to execute on schedule or on trigger
- however it may advantageous to wrap the steps into a task, or split the computational steps into more than one task -- this can help with visibility of execution status in the prefect dashboard, and also allows parallezing the tasks easily via the task runners (see [](recipe-spec_task-runner))
- below we show how to write operations wrapped in a task, and then parallelize it in a flow

for the bullet point below each one will be a subheading at ### level
- add the computational step in /operations
    - use a simple code like add a row that contains the current time to a csv file every time the flow is run
- add a task that wraps the operation
- add a flow that runs the task
- specify a recipe that runs this flow with a DaskTaskRunner every 5 mins
- add expected outputs in the csv file






## Things to consider when adding functions




