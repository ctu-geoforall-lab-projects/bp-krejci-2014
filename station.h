#ifndef STATION_H
#define STATION_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
using namespace std;
class Station
{  
private:
    struct StationList
    {
        string name;
        double lat;
        double lon;
        double elev;
    };
public:

    vector<StationList> station_list;

    vector<StationList>&  returnStationsList(){return station_list;}
    void readListStation(string &read_station_path);
    Station(void);


};

#endif // STATION_H
