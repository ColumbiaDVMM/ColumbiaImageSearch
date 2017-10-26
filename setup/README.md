# Docker installation

This folder contains the Docker files and setup scripts for each of the components of the system:

- [Image downloading](KafkaImageDownloader)
- [Full image processing](SentibankPyCaffeImageProcessing)
- [Full image search](SentibankPyCaffeImageSearch)
- [Face processing](DLibFaceProcessing)
- [Face search](DLibFaceSearch)

Each component would be better run on a separate machine and you might need to adjust 
some parameters (number of workers, number of threads etc.) to the hardware you use,
to optimize the computation or avoid overloading the machines you are using. 
Default `release` parameters are somewhat optimized to the expected hardware and computation load required 
for each component for the MEMEX transition.

More details are available in each folder's README.md file. 