#!/bin/bash

curl -X POST  -H "Accept: Application/json" -H "Content-Type: application/json" http://api.stutzthings.com/v1/test/tracker -d '{"account_password":"test", "custom_name":"Moto 4"}'
