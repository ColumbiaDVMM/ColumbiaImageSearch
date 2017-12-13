## Image processing pipeline

The `docker-compose` file in this directory will start an image procesing pipeline based on the configuration
defined in the `.env` file. 

Copy (and edit) one of the example environment file `.env_test_dlib`, `.env_test_sb`, `.env_release_dlib` or 
`.env_release_db` to `.env`.

And then run:

```(sudo) docker-compose up```

Once everything is running you can detach the docker-compose process by pressing `Ctrl + z`.
You can check the docker logs with the command:

```(sudo) docker logs -f [docker-container-name]```

The `docker-container-name` should be `process_img_processor_1` and you can check it with `docker ps`.
 
