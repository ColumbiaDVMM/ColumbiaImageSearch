# Host webserver setup

Here is some help on how to setup the webserver on the host machine.

### Apache modules

You need to activate the modules 'Headers', 'Rewrite Engine' and 'Reverse Proxy' with the following commands:

- `sudo a2enmod headers`
- `sudo a2enmod rewrite`
- `sudo a2enmod proxy`
- `sudo a2enmod proxy_http`

Then restart Apache: `sudo service apache2 restart`
