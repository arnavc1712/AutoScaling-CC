import paramiko
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('ec2-35-170-202-156.compute-1.amazonaws.com',
            username='ubuntu',
            key_filename='''ec2-keypair.pem''')
stdin, stdout, stderr = ssh.exec_command("Xvfb :1 & export DISPLAY=:1; echo $DISPLAY")
stdin.flush()
data = stdout.read().splitlines()
for line in data:
    print(line)
ssh.close()