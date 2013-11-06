#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include "datastation.h"
#include "snoss2d.h"

#include <stdio.h>
using namespace std;

//info o stanicich
//http://www.wcc.nrcs.usda.gov/ftpref/data/snow/snotel/snothist/listak.txt

using namespace std;

int main()
{
    DataStation dataS;
    Station stationLoaded;
    dataS.setFirstStart(true);
    string read_station_path= "stations.txt";

    //-------------------PRAPARE DATA--------------------------------------------

    //download z unix console
    system("wget  http://www.wcc.nrcs.usda.gov/ftpref/data/snow/snotel/tk/24hr_reports/ak24hr.txt");

//    change file name to curr date
    string currdate= dataS.currentDate();
    cerr<<currdate<<endl;
    int result;
    string oldname="ak24hr.txt";
    string newname =currdate+".txt";
    rename(oldname.c_str(),newname.c_str());

    if ( result == 0 )
        puts ( "File successfully renamed" );
    else
        perror( "Error renaming file" );
    //-------------------Load data to struct--------------------------------------------

    stationLoaded.readListStation(read_station_path);      //nahraju stanice z "station.txt
    dataS.readData(stationLoaded,newname);

    //check data
    vector <DataStation::sDataStation> data_loaded = dataS.data_loaded;
    for(int i = 0;i<data_loaded.size();i++ )
    {
        cerr << data_loaded[i].station_name << endl;
        for(int j=0;j<data_loaded[i].date.size();j++ )
        {
            cerr << data_loaded[i].date[j] << " " << data_loaded[i].snow_eq[j] << " " << data_loaded[i].precip[j] << " " << data_loaded[i].temp[j] << " " << data_loaded[i].snow_deep[j] << endl;
        }
        cerr << endl;
    }
    //-------------------RUN SNOSS-----------------------------------------------
    Snoss2D snoss;
    snoss.snoss2Dbatch(dataS);

    // data.clear();
}

