#!/bin/bash

# Install needed modules
sudo a2enmod headers
sudo a2enmod rewrite
sudo a2enmod proxy
sudo a2enmod proxy_http

# Copy conf file
sudo cp ./000-default.conf /etc/apache2/sites-enabled/

# Restart Apache
sudo service apache2 restart
