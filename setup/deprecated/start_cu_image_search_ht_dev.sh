DOCKER_NVIDIA_DEVICES="--device /dev/nvidia0:/dev/nvidia0 --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm"
docker stop columbia_university_image_search_ht_dev
docker rm columbia_university_image_search_ht_dev
docker run -p 87:3006 -p 86:5000 -v /srv/skaraman/cu_image_search_ht_dev:/home/ubuntu/memex --cap-add IPC_LOCK --name=columbia_university_image_search_ht_dev -ti $DOCKER_NVIDIA_DEVICES digmemex/columbia_university_image_search_ht_dev:3.0 /bin/bash
