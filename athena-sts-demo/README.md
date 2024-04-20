Demo Springboot and AWS project to connect to Athena and Query data (Connect to AWS with an User profile)

create an AWS account.

(you can use terrform to create infra)

create an IAM user and role 

create S3 bucket for Athena 

Create Athena Table

================================

Install docker desktop

Install and run minikuke 

 run minikube using following command 

    minikube start


Run the docker Image in the K8S
================================

1) First build the project clean-install

2) docker build -t athena-sts-demo .

3) docker tag athena-sts-demo gourabp/gp-aws-athena-sts

4) docker push gourabp/gp-aws-athena-sts

5) kubectl apply -f namespace.yaml

6) kubectl apply -f docker-secrets.yaml

7) kubectl apply -f aws-credentials-secret.yaml

8) kubectl apply -f deployment.yaml
