FROM dock0/arch
RUN useradd -m -G wheel -s /bin/bash archie
RUN echo '%wheel ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
RUN pacman -Sy diffutils coreutils gcc awk fakeroot file sudo binutils make vim python2-numpy python2-scipy python2-scikit-learn --noconfirm
RUN echo 'export SPARK_HOME=/usr/share/apache-spark' >> /etc/bash.bashrc 
USER archie
WORKDIR /tmp
RUN git clone https://aur.archlinux.org/package-query.git; git clone https://aur.archlinux.org/yaourt.git
WORKDIR /tmp/package-query
RUN makepkg -si --noconfirm
WORKDIR /tmp/yaourt
RUN makepkg -si --noconfirm
RUN yaourt -S apache-spark --noconfirm
USER root
