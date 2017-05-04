DOCKER_NVIDIA_DEVICES="--device /dev/nvidia0:/dev/nvidia0 --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm"
docker stop columbia_university_image_search_ht
docker rm columbia_university_image_search_ht
docker run -p 84:3006 -p 85:5000 -v /srv/skaraman/cu_image_search_ht:/home/ubuntu/memex -v /srv/tchen/notcached_update:/home/ubuntu/notcached_update --cap-add IPC_LOCK --name=columbia_university_image_search_ht -ti $DOCKER_NVIDIA_DEVICES digmemex/columbia_university_image_search_ht:2.1 /bin/bash
