#include "station.h"

Station::Station(void)
{
}




void Station::readListStation(string &read_station_path_s)
{

    char *read_station_path=new char[read_station_path_s.size()+1];
    read_station_path[read_station_path_s.size()]=0;
    memcpy(read_station_path,read_station_path_s.c_str(),read_station_path_s.size());

    ifstream inp(read_station_path);
    int c=0;
    StationList line;
    while (inp)
    {
        inp>>line.name >>line.lat>>line.lon>>line.elev;
        station_list.push_back(line);
        c++;
        if( inp.eof() ) break;
    }
    //replace  "_" for " " in  station name
    for(int i=0;i<c;i++)
    {
        string name=station_list[i].name;
        replace(name.begin(), name.end(), '_', ' ');
        name.erase(remove_if(name.begin(), name.end(), ::isspace), name.end());
        station_list[i].name=name;
    }

    for(int i =0;i<station_list.size();i++)
    {
        cerr<<station_list[i].name<<" ";
        cerr<<station_list[i].lat<<" ";
        cerr<<station_list[i].lon<<" ";
        cerr<<station_list[i].elev<<" ";
        cerr<<endl;
    }

}

