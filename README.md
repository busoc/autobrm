# autobrm
Autobrm is a tool to fill FSL HRD archive gaps

Autobrm can be found on the FSL-Yamcs-PDC virtual marchine on the local Mars PDC. It's a tool written in Python that uses Meex to scan the archive for gaps. Once gaps have been located it merges/optimises the datetime ranges and finally calls the command line bitstream request tool to issue replays to the AOS/LOS 72h Col-CC buffer.
Autobrm uses python 2.7 and it's located inside the folder /opt/autobrm.
