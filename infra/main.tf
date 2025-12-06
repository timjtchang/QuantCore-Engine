provider "aws" {
  region = "us-west-1"
}

# --- 1. NETWORK LOOKUP (The Fix) ---
# Explicitly look up the Default VPC
data "aws_vpc" "default" {
  default = true
}

# Explicitly look up subnets within that specific VPC
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# --- 2. AMI LOOKUP ---
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --- 3. SECURITY GROUP ---
resource "aws_security_group" "quantcore_sg" {
  name        = "quantcore-sg"
  description = "Allow SSH, Spark, and API traffic"
  
  # CRITICAL FIX: Force SG into the explicitly found VPC
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- 4. EC2 INSTANCE ---
resource "aws_instance" "quantcore_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "m5.xlarge"
  key_name      = "quantcore-key" # Make sure this matches your AWS Console key name!

  # Connect the SG
  vpc_security_group_ids = [aws_security_group.quantcore_sg.id]
  
  # CRITICAL FIX: Force Instance into a Subnet within the SAME VPC
  subnet_id = data.aws_subnets.default.ids[0]

  tags = {
    Name = "QuantCore-Engine-Prod"
  }

  user_data = <<-EOF
              #!/bin/bash
              echo "Installing Docker..."
              apt-get update
              apt-get install -y docker.io docker-compose git python3-pip
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ubuntu

              echo "Install python dependencies"

              sudo apt install python3-pip
              pip3 install --no-cache-dir \
                  --only-binary :all: \
                  confluent-kafka simplejson websocket-client redis


              echo "Cloning Repo..."
              git clone https://github.com/timjtchang/QuantCore-Engine.git /home/ubuntu/quantcore

              echo "Starting QuantCore..."
              cd /home/ubuntu/quantcore
              docker-compose up -d

              echo "Setting up kafak..."
              sudo docker exec -it kafka kafka-topics --create \
                --topic order_book \
                --bootstrap-server localhost:9092 \
                --partitions 30 \
                --replication-factor 1
              
              EOF
}

output "server_public_ip" {
  value = aws_instance.quantcore_server.public_ip
}