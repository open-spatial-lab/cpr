# Authenticate Docker with ECR
aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 345225490760.dkr.ecr.us-west-1.amazonaws.com

# Build and tag your image
docker build --platform linux/amd64 -t pan-data-update .
docker tag pan-data-update:latest 345225490760.dkr.ecr.us-west-1.amazonaws.com/pan-data-update:latest

# Push to ECR
docker push 345225490760.dkr.ecr.us-west-1.amazonaws.com/pan-data-update:latest