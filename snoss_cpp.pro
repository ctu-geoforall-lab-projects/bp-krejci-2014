TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += main.cpp \
    data.cpp \
    datastation.cpp \
    snoss2d.cpp \
    station.cpp

OTHER_FILES += \
    params.txt \
    ReadMe.txt \
    stations.txt

HEADERS += \
    data.h \
    datastation.h \
    snoss2d.h \
    station.h

