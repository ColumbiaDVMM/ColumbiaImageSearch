# Docker installation

This folder contains the Docker files and setup scripts for each of the components of the system:

- [Image downloading](KafkaImageDownloader)
- [Full image processing](SentibankPyCaffeImageProcessing)
- [Face processing](DLibFaceProcessing)
- [Full image search](SentibankPyCaffeImageSearch)
- [Face search](DLibFaceSearch)

Each component should be run in a separate machine and you might need to adjust 
some parameters (number of workers, number of threads etc.) to the hardware you use,
to optimize the computation or avoid overloading the machines you are using. 
Default parameters are somewhat optimized to the expected hardware and computation load required 
for each component for the MEMEX transition.

More details are available in each folder's readme. 