
function [results, r] = run_SNOSS_2D_batch(params)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% !! ONLY RUN ONE STATION AT A TIME !!
%
% INPUTS:
%
%   wxdata.     % weather data structure in this format
%           Station
%           StationInfo
%           Time - in matlab serial
%           Precip - [mm]
%           HrPrecip - [mm/hr]
%           Temperature - [deg C]
%           SnowDepth - [m]
%           SWE1 - [m]
%           SWE2 - [m]
%           SWin - [W/m2]
%           SWout - [W/m2]
%           LWin - [W/m2]
%           LWout - [W/m2]
%           SurfaceTemp - [deg C]
%
%   params.   %constants for SNOSS
%       A1
%       A2
%       B1
%       B2
%       theta
%       sigm
%
%
% 20110320 Scott Havens
% Final form: 20110404
% Now includes SNOSS1D_RT_v3.m, SNOSS1D_RT_initialize_v2.m,
% plot_SNOSS_results.m as subfunction to keep the scripts in one location
% and for faster processing time
%
% Update 20110405 SCH - trying to not have to save variable M, but create
% it each time.  May be slower in processing but will cut half the size of
% the output file.
%
% Update 20110419 SCH - If missing temperature data (NAN from
% mesowest_realtime.m) then assume that density if 8% with snow
% temperature of -7 deg C.  This is only a good assumption for Banner
% Summit during the winter.
%
% Wrapper script to run SNOSS Real Time.
% This calls SNOSS1D_RT_v3.m and SNOSS1D_RT_initialize_v2.m.  Loads data from the
% real time mesomest or snotel data.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
wxdata=importdata('WxData.mat')

% set parameters
params.A1 = 1.6; %precip enhancement
params.A2 = 19500; %shear fracture constant
params.theta = 40; %slope angle
params.sigm = 75; % metamorphic stress
params.B1 = 2.6953e-8; %compactive viscosity constant
params.B2 = 30.27; %compactive viscosity constant - controls exponentional decay of depth
params.SIcrit = 1; %critical value of the stability index
% ------------------------------------------------------------------------------

% preallocate results
p = sum(wxdata.HrPrecip > 0); %number of layers
n = length(wxdata.Time);

% r stucture is SNOSS results that is used while running SNOSS
r = struct('daydec',[],'daydeposit',[],'P',[],'TP',[],'Ts',[],'rho',[],'nzz',[],'sigfz',[],'sigxz',[],...
    'sigzz',[],'stabindex',[],'tf',[],'thickness',[],'dz',[],'snowdepth',[],...
    'swe',[]);

% results structure is an output structure that will be used for creating
% figures of the results
results = struct('daydec',wxdata.Time,'rho',NaN(p,n),'nzz',NaN(p,n-1),...
    'sigfz',NaN(p,n),'sigxz',NaN(p,n), 'sigzz',NaN(p,n),...
    'stabindex',NaN(p,n),'tf',NaN(p,n-1),'thickness',NaN(p,n),...
    'dz',NaN(p,n),'snowdepth',NaN(1,n),'swe',NaN(1,n));

% params.B1 = B1;
% params.B2 = B2;


% RUN SNOSS
for k = 1:length(wxdata.Time)
    
    % create temporary data structure to pass to SNOSS1D_RT in the
    % event of more than one time step needing to be run
    data2.daydec = wxdata.Time(k);
    data2.precip = params.A1*wxdata.HrPrecip(k);
    data2.temp = wxdata.Temperature(k);
    data2.p0 = 134.2.*exp(19.95.*data2.temp./(273+data2.temp)); % initial density estimate p_0
   
    
    %if first precip event, initialize SNOSS
    if data2.precip > 0 && isempty(r.TP)
        r = SNOSS1D_RT_initialize_v2(data2,r,params);
        
        %else if precipitation and snow on ground
    elseif data2.precip > 0
        r = SNOSS1D_RT_v3(data2,r,params);
        
        %no precip but snow on ground
    elseif data2.precip == 0 && ~isempty(r.TP)
        data2.p0 = NaN;
        r = SNOSS1D_RT_v3(data2,r,params);
        
        % no precip and no snow on ground
    elseif data2.precip == 0 && isempty(r.TP)
        % store daydec of all data downloaded
        r.daydec = [r.daydec;...
            data2.daydec];
    end
    
    % Update the results data structure for plotting
    if data2.precip == 0 && isempty(r.TP)
        % no precip and no snow on ground, don't store anything
        results.snowdepth(1,k) = 0;
        results.swe(1,k) = 0;
    else
        f = fieldnames(results); %fieldnames of output structure
        for m = 1:length(f)
            clear tmp tmp2 tmp3
            
            if ~strcmp(f{m},'daydec')
                if strcmp(f{m},'snowdepth') || strcmp(f{m},'swe')
                    results.(f{m})(1,k) = r.(f{m})(:,end);
                else
                    results.(f{m})(p-length(r.(f{m})(:,end))+1:end,k) = r.(f{m})(:,end);
                end
            
            end
            
            
            
%             tmp = r.(f{m}); %get current time step outputs
%             tmp2 = results.(f{m}); %existing storage matrix values
%             
%             if isempty(tmp2) %first layer of season
%                 
%                 if strcmp(f{m},'daydec') %start results time when first layer starts
%                     results.(f{m}) = data2.daydec;
%                 else
%                     results.(f{m}) = tmp;
%                 end
%                 
%             elseif strcmp(f{m},'daydec')
%                 results.(f{m}) = [results.(f{m}); data2.daydec];
%                 
%             elseif strcmp(f{m},'snowdepth')
%                 results.(f{m}) = [results.(f{m}); tmp];
%                 
%             else
%                 h = length(r.thickness) - size(tmp2,1); %should be 1 or 0
%                 wd = size(tmp2,2); %width of output matrix
%                 tmp3  = [NaN(h,wd); tmp2];
%                 tmp = [NaN(length(r.thickness)-size(tmp,1),1); tmp];
%                 results.(f{m}) = [tmp3 tmp]; %pad with NaN's on top and restore
%                 
%             end
            
        end
    end
    
end

% save(ResultsPath,'r','results');

% perform smoothing and plot results
% plot only if there is more than 20 cm of snow
% if max(results.snowdepth) > 0.2
%     plot_SNOSS_results(results,...
%         FigurePath,...
%         stations);
% end
% end
% end

% save(ResultsPath,'r','results');

end

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% SUBFUNCTIONS
%   - SNOSS1D_RT_initialize_v2
%   - SNOSS1D_RT_v3
%   - plot_SNOSS_results
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

function r = SNOSS1D_RT_initialize_v2(data,r,params)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Updated from SNOSS1D_v3.M to run hourly
% This function is only for the first precipitation event to initialize
% SNOSS
% SCH 20101105
%
% HPM  02/12/03, updated 07/06/04, 01/12/07, 08/14/08
% this function calculates the 1D SNOSS results and plots
%  stress/strength, stability index, and time to failure
%
% INPUT: data = structure array containing data
%            .daydec = day.dec  (time)
%            .precip = [inches] hourly precip
%            .temp = [deg C] hourly air temperature (constant snow temp in initial tests)
%            .p0 = [kg/m^3] initial density
%
%        r: structure of SNOSS output results from previous time steps with
%                     the top of snow as the first element.
%                     running updates this array
%               r.daydec = day.dec vector (all time steps)
%               r.P = [m] hourly precip for each layer
%               r.TP = [m] total precip for each layer
%               r.Ts = [deg C] air temperature when layer was deposited
%               r.nzz = [Pa*s] compactive viscosity
%               r.rho = [kg/m^3] density evolution
%               r.dz = [m] change in layer height
%               r.thickness = [m] thickness of layer
%               r.sigfz = [Pa] shear strength
%               r.sigxz = [Pa] shear stress
%               r.sigzz = [Pa] vertical overburden stress
%               r.stabindex = [] stability index (strength/stress)
%               r.tf = [hr] time to failure
%               r.swe = [m] total snow water equivalent
%
%        params = (optional) structure array containing model parameters
%              .A1 = [] (1) precip enhancement factor
%              .theta [deg] (40) slope angle [deg]
%              .sigm = [Pa] (75) metamorphic stress, negative for TG metamorphism
%              .A2 = [Pa] (1.95e4) constant in strength/density relationship, lower for facets, eq. 3, Conway and Wilbour, CRST 1999
%              .B1 = [Pa s] (6.5e-7) constant in compactive viscosity equation, eq. 5, Conway and Wilbour, CRST, 1999
%              .B2 = [] (19.3) constant in eq. 5
%
% OUTPUT: r : structure of SNOSS output results from current time step
%               r.daydec = day.dec vector (all time steps)
%               r.P = [m] hourly precip for each layer
%               r.TP = [m] total precip for each layer
%               r.Ts = [deg C] air temperature when layer was deposited
%               r.nzz = [Pa*s] compactive viscosity
%               r.rho = [kg/m^3] density evolution
%               r.dz = [m] change in layer height
%               r.thickness = [m] thickness of layer
%               r.sigfz = [Pa] shear strength
%               r.sigxz = [Pa] shear stress
%               r.sigzz = [Pa] vertical overburden stress
%               r.stabindex = [] stability index (strength/stress)
%               r.tf = [hr] time to failure
%               r.swe = [m] total snow water equivalent
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% DEFINE CONSTANTS
g = 9.8; % [m/s^2] gravitational constant,
% E = 67.3; % [kJ/mol] activation energy
% R = 0.0083; % [kJ/mol/K] gas constant
p_ice = 917; % [kg/m^3] density of ice
pw = 1000; % [kg/m^3] density of water
% dt = 3600; % hourly time step [s]

% if there is precip, create new layer and update variables dependent on
%    total precipitation
if data.precip > 0
    
    % calculate affect of new precip on cumulative
    r.TP = data.precip;
    
    % Calculate Basal Shear Stress evolution
    r.sigxz = g*r.TP*cosd(params.theta)*sind(params.theta)*pw;  % [Pa] overburden shear stress at basal layer, eq. 1 CRST 1999
    
    % Calculate vertical stress from overburden
    r.sigzz = g*r.TP*(cosd(params.theta))^2*pw; % overburden vertical stress sig_zz
    
    % [m] layer thickness
    r.thickness = data.precip(end)*pw/data.p0;
    
    % [m] max snow depth
    r.snowdepth = sum(r.thickness);
    
    % [m] evolution of layer thickness,
    r.dz = 0; %  top doesn't change thickness if new snow
    
    % update variables with new layer info
    r.rho = data.p0; %exclude the new snow from densification
    r.P = data.precip;
    r.Ts = data.temp; %update layer temperature [deg C]
    
    % Calculate shear fracture strength evolution of basal layer
    r.sigfz = params.A2.*(r.rho./p_ice).^2;  % [Pa] shear fracture strength of basal layer, eq. 3
    
    % Calculate stability index, eq. 6 CRST 1999
    si_tmp = r.sigfz./r.sigxz; % stability index [] (strength/stress)
    % stabindex = [stabindex(:,2); NaN(length(tmp)-
    r.stabindex = si_tmp*ones(1,3); % 'create' three times step priod
    
    % Calculate time to failure, eq 7 CRST 1999
%     r.stabindex = si_tmp;
    
    % [m] total snow water equavalent
    r.swe = sum(r.thickness.*r.rho./1000);
    
    % set place holders for nzz and tf
    r.nzz = NaN;
    r.tf = NaN;
    
else % else use the same variables from previous time step
    
    error('Ran wrong initialization code!');
    
end

r.daydec = [r.daydec; data.daydec];
r.daydeposit = data.daydec;

end


function r = SNOSS1D_RT_v3(data,r,params)

% Updated from SNOSS1D_v3.M to run hourly
% SCH 20101105
%
% HPM  02/12/03, updated 07/06/04, 01/12/07, 08/14/08
% this function calculates the 1D SNOSS results and plots
%  stress/strength, stability index, and time to failure
%
% INPUT: data = structure array containing data
%            .daydec = day.dec  (time)
%            .precip = [inches] hourly precip
%            .temp = [deg C] hourly air temperature (constant snow temp in initial tests)
%            .p0 = [kg/m^3] initial density
%
%        r: structure of SNOSS output results from previous time steps with
%                     the top of snow as the first element.
%                     running updates this array
%               r.daydec = day.dec vector (all time steps)
%               r.P = [m] hourly precip for each layer
%               r.TP = [m] total precip for each layer
%               r.Ts = [deg C] air temperature when layer was deposited
%               r.nzz = [Pa*s] compactive viscosity
%               r.rho = [kg/m^3] density evolution
%               r.dz = [m] change in layer height
%               r.thickness = [m] thickness of layer
%               r.sigfz = [Pa] shear strength
%               r.sigxz = [Pa] shear stress
%               r.sigzz = [Pa] vertical overburden stress
%               r.stabindex = [] stability index (strength/stress)
%               r.tf = [hr] time to failure
%               r.swe = [m] total snow water equivalent
%
%        params = (optional) structure array containing model parameters
%              .A1 = [] (1) precip enhancement factor
%              .theta [deg] (40) slope angle [deg]
%              .sigm = [Pa] (75) metamorphic stress, negative for TG metamorphism
%              .A2 = [Pa] (1.95e4) constant in strength/density relationship, lower for facets, eq. 3, Conway and Wilbour, CRST 1999
%              .B1 = [Pa s] (6.5e-7) constant in compactive viscosity equation, eq. 5, Conway and Wilbour, CRST, 1999
%              .B2 = [] (19.3) constant in eq. 5
%
% OUTPUT: r : structure of SNOSS output results from current time step
%               r.daydec = day.dec vector (all time steps)
%               r.P = [m] hourly precip for each layer
%               r.TP = [m] total precip for each layer
%               r.Ts = [deg C] air temperature when layer was deposited
%               r.nzz = [Pa*s] compactive viscosity
%               r.rho = [kg/m^3] density evolution
%               r.dz = [m] change in layer height
%               r.thickness = [m] thickness of layer
%               r.sigfz = [Pa] shear strength
%               r.sigxz = [Pa] shear stress
%               r.sigzz = [Pa] vertical overburden stress
%               r.stabindex = [] stability index (strength/stress)
%               r.tf = [hr] time to failure
%               r.swe = [m] total snow water equivalent
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% DEFINE CONSTANTS
g = 9.8; % [m/s^2] gravitational constant,
E = 67.3; % [kJ/mol] activation energy
R = 0.0083; % [kJ/mol/K] gas constant
p_ice = 917; % [kg/m^3] density of ice
pw = 1000; % [kg/m^3] density of water

% calculate dt since last time SNOSS was run
dt = (data.daydec-r.daydec(end))*24*3600; % time step [s]

% if there is precip, create new layer and update variables dependent on
%    total precipitation
% this will not calculate densification and time to failure on the new
% snow layer
if data.precip > 0
    
    % calculate affect of new precip on cumulative
    r.TP = [data.precip; data.precip+r.TP];
    r.P = [data.precip; r.P];
    
    % Calculate Basal Shear Stress evolution
    r.sigxz = g*r.TP*cosd(params.theta)*sind(params.theta)*pw;  % [Pa] overburden shear stress at basal layer, eq. 1 CRST 1999
    
    % Calculate vertical stress from overburden
    r.sigzz = g*r.TP*(cosd(params.theta))^2*pw; % overburden vertical stress sig_zz
    
    % calculate compactive viscosity n_zz
    r.nzz = params.B1.*exp(params.B2.*r.rho./p_ice).*exp(E./(R.*(r.Ts+273.15))); % [Pa*s] compactive viscosity , eq.5
    
    % Calculate the density evolution, eq.4 CRST 1999
    r.rho = r.rho.*(1 + (1./r.nzz).*(params.sigm + r.sigzz(2:end)).*dt); % update density [kg/m3]
    
    % [m] layer thickness
    tmp = [r.P(1).*pw./data.p0; r.P(2:end).*pw./r.rho];
    
    % [m] evolution of layer thickness,
    r.dz = [0; r.thickness - tmp(2:end)]; %  top doesn't change thickness if new snow
    
    r.thickness = tmp;
    
    % [m] snow depth
    r.snowdepth = sum(r.thickness);
    
    % update variables with new layer info
    r.rho = [data.p0; r.rho]; %exclude the new snow from densification
    r.Ts = [data.temp; r.Ts]; %update layer temperature [deg C]
    
    % Calculate shear fracture strength evolution of basal layer
    r.sigfz = params.A2.*(r.rho./p_ice).^2;  % [Pa] shear fracture strength of basal layer, eq. 3
    
    % Calculate stability index, eq. 6 CRST 1999
    si_tmp = r.sigfz./r.sigxz; % stability index [] (strength/stress)
    r.stabindex(:,1) = []; %get rid of first column to add new stability index to end
    r.stabindex(:,3) = si_tmp(2:end); % add the new stability index to the older layers
    r.stabindex = [si_tmp(1)*ones(1,3); r.stabindex]; % add the first layer
        
    % Calculate time to failure, eq 7 CRST 1999
    % use the previous three stability index time steps to determine slope
    x = 1:size(r.stabindex,2); % get dummy time variables
    nlay = size(r.stabindex,1)-1; % number of layer to calculate tf
    A = [x' ones(numel(x),1)];
    
    for n = 1:nlay
        y = r.stabindex(n+1,:); %fit a line to this layer's SI
        % p = polyfit(x,y-1,1); % get the slope and intercept
        p = A\(y'-params.SIcrit);
        
        if y(end) < params.SIcrit % currently not stable
            r.tf(n,1) = 1e-3;
        else % currently stable
            if p(1) < 0 % decreasing stability
                r.tf(n,1) = -p(2)/p(1); %means SI is decreasing
            else
                r.tf(n,1) = 1000; % increasing stability
            end
        end
    end
    
    % store the day the layer was deposited
    r.daydeposit = [r.daydeposit; data.daydec];
    
    % [m] total snow water equavalent
    r.swe = sum(r.thickness.*r.rho./1000);
    
else % else use the same variables from previous time step and calculate the
    % temporal layer evolution
    
    % calculate compactive viscosity n_zz
    r.nzz = params.B1.*exp(params.B2.*r.rho./p_ice).*exp(E./(R.*(r.Ts+273.15))); % [Pa*s] compactive viscosity , eq.5
    
    % Calculate the density evolution, eq.4 CRST 1999
    r.rho = r.rho.*(1 + (1./r.nzz).*(params.sigm + r.sigzz).*dt); % update density [kg/m3]
    
    % [m] layer thickness
    tmp = r.P.*pw./r.rho;
    
    % [m] evolution of layer thickness,
    r.dz = r.thickness - tmp; %  top doesn't change thickness if new snow
    
    r.thickness = tmp;
    
    % [m] snow depth
    r.snowdepth = sum(r.thickness);
    
    % Calculate shear fracture strength evolution of basal layer
    r.sigfz = params.A2.*(r.rho./p_ice).^2;  % [Pa] shear fracture strength of basal layer, eq. 3
    
    % Calculate stability index, eq. 6 CRST 1999
    si_tmp = r.sigfz./r.sigxz; % stability index [] (strength/stress)
    r.stabindex(:,1) = []; %get rid of first column to add new stability index to end
    r.stabindex(:,3) = si_tmp; % add the new stability index to the older layers
        
    % Calculate time to failure, eq 7 CRST 1999
    % use the previous three stability index time steps to determine slope
    x = 1:size(r.stabindex,2); % get dummy time variables
    nlay = size(r.stabindex,1); % number of layer to calculate tf
    A = [x' ones(numel(x),1)];
    
    for n = 1:nlay
        y = r.stabindex(n,:); %fit a line to this layer's SI
        % p = polyfit(x,y-1,1); % get the slope and intercept
        p = A\(y'-params.SIcrit);
        
        if y(end) < params.SIcrit % currently not stable
            r.tf(n,1) = 1e-3;
        else % currently stable
            if p(1) < 0 % decreasing stability
                r.tf(n,1) = -p(2)/p(1); %means SI is decreasing
            else
                r.tf(n,1) = 1000; % increasing stability
            end
        end
    end
    
    % [m] total snow water equavalent
    r.swe = sum(r.thickness.*r.rho./1000);
    
end

% update dates SNOSS was ran
r.daydec = [r.daydec; data.daydec];

% ind3 = find(r.tf<0); %no negative time to failure
r.tf(r.tf<0) = 1e-5;
end

















