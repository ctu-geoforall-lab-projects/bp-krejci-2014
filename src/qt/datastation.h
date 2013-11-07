#ifndef DATASTATION_H
#define DATASTATION_H
#include <string>
#include <vector>
#include <cmath>
#include <stdio.h>
#include <limits>
#include <fstream>
#include <ios>
#include <iostream>
#include <sstream>
#include "station.h"
#include <algorithm>

class DataStation: public Station
{
private:

    string l_station_name;
    int    l_date;
    double l_precip;
    double l_snow_deep;
    double l_snow_eq;
    int station_pos;
    bool first_start;

public:
    struct sDataStation
    {
        string station_name;
        vector <int> date;
        vector <double> precip;
        vector <double> temp;
        vector <double> snow_deep;
        vector <double> snow_eq;
        bool b_precip;
        bool b_temp;
        bool b_show_deep;
        bool b_show_eq;
    };

    sDataStation tmp_data_loaded;               //pouze prechodny soubor
    string currentDate();
    vector <sDataStation> data_loaded;          //zde budou nactena vsechna data vsech stanic

//fce na upravu dat

    bool getFirstStart(){return first_start;}

    void setFirstStart(bool  first_start_){first_start=first_start_;}

    fstream& gotoLine(fstream& file,  int num);

    double makeNumber(string& inp);

    string replaceString(string & name,char& This,char& ForThis);

    vector<int> dateToVec(string& dateToConv);

    int julday (vector<int>&yymmddhh);

//    dve hlavni fce na nacitani dat do struktury
    void readData (Station& a ,string& read_data_path);

    void fillDataStation(string& curr_station,int& countline,string& read_data_path);



    DataStation(void);
    ~DataStation(void);
};

#endif
