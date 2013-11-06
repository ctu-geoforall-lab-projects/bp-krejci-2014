#include "datastation.h"


DataStation::DataStation(void)
{

}

//---------------------------------funkce na upravu dat---------------------

string DataStation::currentDate()
{
    time_t t = time(0);   // get time now
    struct tm * now = localtime( & t );

    int a=( now->tm_year + 1900);
    int b=( now->tm_mday);
    int c=( now->tm_mon + 1) ;
    int d=( now->tm_hour);
    int e=( now->tm_min);
    char currentDate[30];
    sprintf(currentDate, "%02d-%02d-%d-%d%d",a, b, c, d,e);
    return currentDate; // it will automatically be converted to string
}


int DataStation::julday (vector<int>&yymmddhh)
{
    int iyyy = yymmddhh[0] + 2000;
    int id   = yymmddhh[1];
    int mm   = yymmddhh[2];
    int hr   = yymmddhh[3];

    const int IGREG = 15 + 31*(10 + 12*1582);
    int ja,jul,jy = iyyy,jm;

    if (jy   ==  0) cerr << ("julday: there is no year zero.");
    if (jy < 0) ++ jy;
    if (mm > 2) {
        jm = mm + 1;
    } else {
        --jy;
        jm = mm + 13;
    }
    jul = int(floor(365.25*jy) + floor(30.6001*jm) + id + 1720995);
    if (id + 31*(mm + 12*iyyy) >= IGREG) {
        ja = int(0.01*jy);
        jul  += 2-ja + int(0.25*ja);
    }
    //Convert to hours
    jul = jul*24 + hr;
    return jul;
}

vector<int> DataStation::dateToVec(string& dateToConv)
{
    //-----------------fce na naplneni vektoru datumem ze stringu v poradi rok,mesic,den,hodina--------------------
    vector<int> date;
    string buff;

    buff = string("20") + dateToConv.at(0) + dateToConv.at(1); // year
    //   cerr << buff << endl;
    date.push_back( atoi(buff.c_str()) );

    buff = string("") + dateToConv.at(2) + dateToConv.at(3); // month
    //   cerr << buff << endl;
    date.push_back( atoi(buff.c_str()) );

    buff = string("") + dateToConv.at(4) + dateToConv.at(5); // day
    //   cerr << buff << endl;
    date.push_back( atoi(buff.c_str()) );

    buff = string("") + dateToConv.at(6) + dateToConv.at(7); // hour
    //   cerr << buff << endl;
    date.push_back( atoi(buff.c_str()) );

    return date;
}

string DataStation::replaceString(string & name,char& This,char& ForThis)
{
    replace(name.begin(), name.end(), This, ForThis);
    name.erase(std::remove_if(name.begin(), name.end(),::isspace), name.end()); //smaze whitespace
    return name;
}

double DataStation::makeNumber(string& inp)
{
    //funkce delana na miru pro 24h data snotel
    vector<char> vec;
    vec.push_back('S');
    vec.push_back('V');
    vec.push_back('-');
    char n = ' ';
    string out;
    out = inp;
    for(unsigned int i = 0;i<vec.size();i++ )
    {
        out = replaceString(out,vec[i],n);
    }
    double numOut  =  atof(out.c_str());
    return numOut ;
}

fstream& DataStation::gotoLine(fstream& file,  int num)
{
    file.seekg(std::ios::beg);
    for( int i = 0; i < num - 1; ++ i)
    {
        file.ignore(numeric_limits<streamsize>::max(),'\n');
    }
    return file;
}

//---------------------------------funkce na nacitani dat---------------------
void DataStation::fillDataStation(string& curr_station,int& countline,string& read_data_path)
{
    vector <string> shortcutData;                   //sem dam hlavicky dat ktere budu potrebovat
    shortcutData.push_back("SWEC");//SNOW WATER EQUIVALENT
    shortcutData.push_back("PCPYR");//PRECIP
    shortcutData.push_back("TAIRC");//TEMP
    shortcutData.push_back("SNOWD");//SNOW DEPTH
    vector <string> shortcutFromLine;
    vector <string> dataFromLine;
    string line;
    string line1;
    string a;
    string a1;
    int countSpace = 0;
    int possnow_eq, pos_precip, post_temp, possnow_deep;
    bool first_start = getFirstStart();

    //--------------------------NACTU HLAVICKU A ZJISTIM V KOLIKATYM SLOUPCI JSOU DANY DATA--------------------
    tmp_data_loaded.station_name = curr_station;
    l_station_name = curr_station;

    fstream file(read_data_path.c_str());                            //kde jsou ulozeny stazene data
    getline(gotoLine(file, countline + 2),line);               //jdu  o dva radky pod stanici kde je hlavicka dat a nactu ji do stringu line
    stringstream inpFromLine(line);
    while(inpFromLine>>a)                                    //radek hlavicek dat naplnim do vector<string> po mezerach
    {
        shortcutFromLine.push_back(a);
        countSpace++ ;
    }
    for(int i = 0;i<shortcutFromLine.size();i++ )
    {           //priradim do struktury na kolikatym miste(odeleno mezerama) je dana zkratka
        for(int j = 0;j<shortcutData.size();j++ )
        {
            if(shortcutFromLine[i] == shortcutData[j])         //kdyz narazim na hlavicku poznamenam si na jakem miste v radce je a reknu ze tam je(bool)
            {
                if(j == 0){possnow_eq   = i;tmp_data_loaded.b_show_eq = 1; };
                if(j == 1){pos_precip   = i;tmp_data_loaded.b_precip = 1;}
                if(j == 2){post_temp    = i;tmp_data_loaded.b_temp = 1;}
                if(j == 3){possnow_deep = i;tmp_data_loaded.b_show_deep = 1;};
            }
        }
    }
    //------------------NACITANI DAT a LASTDATA PODLE SLOUPCE DANYHO PARAMETRU V HLAVICE(snow_eq,PRECIP,TEMP,snow_deep)----------------------
    if(tmp_data_loaded.b_precip == 0||tmp_data_loaded.b_temp == 0)
    {
        cerr <<  "stanice " << tmp_data_loaded.station_name << "nema srazky nebo teplotz" << endl;

    }
    vector<int> forJul;
    int numLine = 0;  //kolikatej je to radek od ********* v .txt
    double dataNum = 0;
    int pos = 0;
    int conNum = 0;
    int conNum1 = 0;
    int jul = 0;
    double buff = 0;

    for(int i = countline + 3;i <= countline + 25;i++ )		//od kilikatyho radku v txt
    {
        inpFromLine.clear();
        line1.clear();
        numLine++ ;
        fstream file1(read_data_path.c_str());
        getline(gotoLine(file1, i),line1);
        stringstream inpFromLine1(line1);
        dataFromLine.clear();

        while(inpFromLine1>>a1)					//nahraju do vektoru cislo
        {
            dataFromLine.push_back(a1);
        }
        pos++ ;
        for(int j = 0;j<dataFromLine.size();j++ ) //projedu vsechy data ze radky po mezerach
        {
            if(numLine <= 9)                      //od 9 hodiny je dohromady datum a poradovy cislo v textaku
            {
                conNum = 1;
            }
            else
            {
                //cerr << "10 radek" << endl;
                conNum = 2;
                conNum1 = 0;
            }
            if(j == 0)
            {

                forJul = dateToVec(dataFromLine[j]);      //prevod na julianskej datum a na hodiny
                jul = julday(forJul);
                tmp_data_loaded.date.push_back(jul);
                l_date = jul;
            }

            if(j == possnow_eq-conNum)			//kdyz se jedna o sloupec ze snow_eqvivalent
            {
                if(first_start == true)
                {
                    if(pos == 1)
                    {
                        l_snow_eq = makeNumber(dataFromLine[((j-conNum1))]);
                        dataNum = 0;
                    }else{
                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_snow_eq;
                        l_snow_eq = buff;
                    }
                }
                else
                {
                    if(pos == 1)
                    {

                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_snow_eq;
                        l_snow_eq = buff;
                    }else{
                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_snow_eq;
                        l_snow_eq = buff;
                    }
                }
                //               cerr << dataNum << endl;
                l_snow_eq = makeNumber(dataFromLine[(j-conNum1)]);
                if(dataNum>=0)tmp_data_loaded.snow_eq.push_back(dataNum);
                else tmp_data_loaded.snow_eq.push_back(0);
            }

            if(j == pos_precip-conNum)
            {
                if(first_start == true)
                {
                    if(pos == 1)
                    {
                        l_precip = makeNumber(dataFromLine[(j-conNum1)]);
                        dataNum = 0;
                    }else{
                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_precip;
                        l_precip = buff;
                    }
                }else{
                    if(pos == 1)
                    {

                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_precip;
                        l_precip = buff;
                    }else{
                        dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                        buff = dataNum;
                        dataNum -= l_precip;
                        l_precip = buff;
                    }
                }
                //               cerr << dataNum << endl;
                l_precip = makeNumber(dataFromLine[(j-conNum1)]);
                if(dataNum>=0)tmp_data_loaded.precip.push_back(dataNum);
                else tmp_data_loaded.precip.push_back(0);
            }
            if(j == post_temp-conNum)
            {
                dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                //               cerr << dataNum << endl;
                tmp_data_loaded.temp.push_back(dataNum);
            }
            if(j == possnow_deep-conNum)
            {
                dataNum = makeNumber(dataFromLine[(j-conNum1)]);
                //               cerr << dataNum << endl;
                tmp_data_loaded.snow_deep.push_back(dataNum);
                l_snow_deep = dataNum;
            }
        }
    }
}

void DataStation::readData (Station& a ,string& read_data_path)
{
    ifstream inp(read_data_path.c_str());                   //nactu data z txt
    int countline = 0;
    string line;
    cerr<<endl;
    cerr<<a.station_list.size()<<endl;

    while (getline(inp,line))
    {
        countline++ ;
        line.erase(remove_if(line.begin(), line.end(), ::isspace), line.end());           //vymazu mezery
        for(int j = 0;j<a.station_list.size();j++ )
        {
            tmp_data_loaded.b_precip = 0;
            tmp_data_loaded.b_temp = 0;
            tmp_data_loaded.b_show_deep = 0;
            tmp_data_loaded.b_show_eq = 0;

            if(line == a.station_list[j].name)                //if found station in data.txt
            {

                string curr_station = a.station_list[j].name;
                station_pos = j;
                fillDataStation(curr_station,countline,read_data_path);
                data_loaded.push_back(tmp_data_loaded);

                tmp_data_loaded.date.clear();
                tmp_data_loaded.precip.clear();
                tmp_data_loaded.temp.clear();
                tmp_data_loaded.snow_deep.clear();
                tmp_data_loaded.snow_eq.clear();

            }
        }
    }


}

DataStation::~DataStation(void)
{
}
