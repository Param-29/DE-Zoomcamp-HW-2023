from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/Param-29/DE-Zoomcamp-HW-2023",
    
)

block.save("dev")