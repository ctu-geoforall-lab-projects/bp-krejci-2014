% Run SNOSS and plot results
% Snow and Ice Physics

clear

% set the data path
DataPath = 'WxData.mat';
load(DataPath)

% set parameters
params.A1 = 1.6; %precip enhancement
params.A2 = 19500; %shear fracture constant
params.theta = 40; %slope angle
params.sigm = 75; % metamorphic stress
params.B1 = 2.6953e-8; %compactive viscosity constant
params.B2 = 30.27; %compactive viscosity constant - controls exponentional decay of depth
params.SIcrit = 1; %critical value of the stability index

% run SNOSS
[results, r] = run_SNOSS_2D_batch(w,params);

%plot results
plot_SNOSS_results_batch(results,params,'y');













