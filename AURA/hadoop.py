import os
import pyttsx3  


def hadoop():
    #install hadoop
    if (("install" in p)or("download" in p)) and ("hadoop" in p):
	pyttsx3.speak('Installing Hadoop')
	os.system("wget http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/jdk-8u171-linux-x64.rpm;rpm -i jdk-8u171-linux-x64.rpm")
	os.system("wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1-1.x86_64.rpm;rpm -i hadoop-1.2.1-1.x86_64.rpm --force")
	pyttsx3.speak('Successfully installed')
	print("Successfully installed")

    #Namenode configuration
    elif ("configure" in p) and (("hadoop" in p)or("masternode" in p)or("namenode" in p)):
	#check wheather required softwares are installed or not
	j=os.system("which java")
	h=os.system("which hadoop")
	
	if(j!=0):
		print("Error: Java not found")
		ans=input("Do you want to install jdk[Y/N]: ")
		if(ans=="Y"):
			os.system("wget http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/jdk-8u171-linux-x64.rpm;rpm -i jdk-8u171-linux-x64.rpm")
		else:
			print("Ending.....")
			exit()
	
	if(h!=0):
		print("Error: Hadoop not found")
		pyttsx3.speak('Hadoop not found')
		ans=input("Do you want to install hadoop[Y/N]: ")
		pyttsx3.speak('Do you want to install hadoop')
		if(ans=="Y"):
			os.system("wget https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1-1.x86_64.rpm;rpm -i hadoop-1.2.1-1.x86_64.rpm --force")
		else:
			print("Ending.....")
			exit()
	

	master_ip=input("Enter the ip address of MasterNode/NameNode: ")
	pyttsx3.speak('configuring master node')

	#configuring hdfs-site.xml file
	text='''<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>dfs.name.dir</name>\n<value>/nn</value>\n</property>\n</configuration>'''
	Dfile_hdfs=open("/etc/hadoop/hdfs-site.xml","w")
	Dfile_hdfs.write(text)
	Dfile_hdfs.close()

	#configuring core-site.xml file
	text='''<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>fs.default.name</name>\n<value>hdfs:{}:9001</value>\n</property>\n</configuration>'''
	Dfile_core=open("/etc/hadoop/core-site.xml","w")
	Dfile_core.write(text.format(master_ip))
	Dfile_core.close()
		
	#formatting namenode
	os.system("hadoop namenode -format -y")
	print("Successfully configured...:)")
	pyttsx3.speak('Successfully configured')

    #Datanode configuration
    elif ("configure" in p) and (("hadoop" in p)or("slavenode" in p)or("datanode" in p)):
	#check wheather required softwares are installed or not
	p=os.system("which hadoop")
	q=os.system("which java")
	if(p!=0):
		print("Error: Hadoop not found")
		ans=input("Do you want to install hadoop[Y/N]: ")
		if(ans=="Yes"):
			os.system("rpm -i hadoop-1.2.1-1.x86_64.rpm --force")
		else:
			print("Ending.....")
			exit()
	if(q!=0):
		print("Error: Java not found")
		ans=input("Do you want to install jdk[Y/N]: ")
		if(ans=="Yes"):
			os.system("rpm -i jdk-8u171-linux-x64.rpm")
		else:
			print("Ending.....")
			exit()

	master_ip=input("Enter the ip address of MasterNode/NameNode: ")
	pyttsx3.speak('configuring slave node')

	#configuring hdfs-site.xml file
	text='''<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>dfs.data.dir</name>\n<value>/dn</value>\n</property>\n</configuration>'''
	Dfile_hdfs=open("/etc/hadoop/hdfs-site.xml","w")
	Dfile_hdfs.write(text)
	Dfile_hdfs.close()

	#configuring core-site.xml file
	text='''<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>fs.default.data</name>\n<value>hdfs:{}:9001</value>\n</property>\n</configuration>'''
	Dfile_core=open("/etc/hadoop/core-site.xml","w")
	Dfile_core.write(text.format(master_ip))
	Dfile_core.close()
		
	print("Successfully configured...:)")
	pyttsx3.speak('Successfully configured')

    #start namenode
    elif ("hadoop" in p)and("start" in p)and("master" in p)or("name" in p):
        pyttsx3.speak('starting namenode')
        os.system('hadoop-daemon.sh start namenode')
        pyttsx3.speak("namenode has started")
            
    #start datanode       
    elif("hadoop" in p)and("start" in p)and("slave" in p)or("data" in p):
        pyttsx3.speak("starting namenode")
        os.system("hadoop-daemon.sh start namenode")
        pyttsx3.speak("datanode has started")
