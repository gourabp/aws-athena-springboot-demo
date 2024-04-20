Demo Springboot and AWS project

Run the docker Image in the K8S
================================

1) : First build the project clean-install

2) docker build -t athena-sts-demo .

3) docker tag athena-sts-demo gourabp/gp-aws-athena-sts

4) docker push gourabp/gp-aws-athena-sts

5) kubectl apply -f namespace.yaml

6) kubectl apply -f docker-secrets.yaml

7) kubectl apply -f aws-credentials-secret.yaml

8) kubectl apply -f deployment.yaml
