#!/bin/bash
eval "$(grep '^export S3_' ~/.bashrc)"
exec "$@"
