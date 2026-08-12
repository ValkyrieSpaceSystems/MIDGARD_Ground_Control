#!/bin/bash
git clone https://github.com/nasa/openmct.git
cd ./openmct
npm install
npm audit fix
npm run build