## Image ingestion process

The `docker-compose` file in this directory will start an image ingestion process based on the configuration
defined in the `.env` file. There are currently two types of ingestion supported: 

- `local`: this process reads images from a folder and pushes them to the Kafka topic `images_topic`.
- `kafka`: this process reads the images listed in the field `objects` of a JSON document read from a Kafka topic and pushes them to the Kafka topic `images_topic`. 
The `objects` field is an array of object which fields `obj_stored_url` contains a suffix url (to be prefixed by the `input_obj_stored_prefix` variable) of the image,
and has also a field `content_type` being a string starting with `image` if the object is effectively an image. 
All other fields are ignored.  

Copy (and edit) one of the example environment file `.env_test` or 
`.env_release` to `.env`.

And then run:

```(sudo) docker-compose up```

Once everything is running you can detach the docker-compose process by pressing `Ctrl + z`.
You can check the docker logs with the command:

```(sudo) docker logs -f [docker-container-name]```

The `docker-container-name` should be `ingest_img_pusher_1` and you can check it with `docker ps`.
 
