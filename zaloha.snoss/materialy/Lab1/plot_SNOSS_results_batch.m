function M = plot_SNOSS_results_batch(results,params,disp)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 20110329 Scott Havens
% 20110405 SCH - updated to create matrix each time code is run
% 20110825 SCH - updated take results from the batch run and plot
%
% Create plots for SNOSS
% smooths the last last entry in results for plotting and adds this to
% smoothed plot data.
% THIS ASSUMES THAT YOU HAVE RESULTS FOR EVERY HOUR, NEED TO CHECK THAT
% THERE ARE WX DATA FOR EVERY HOUR

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% get the time vector
M.daydec = results.daydec;

%get the elasped time
telapsed = length(M.daydec);

% get the time indicies
% tind = find(results.daydec' >= stime);

% if stime < results.daydec(1) %not one week of data yet
%     telapsed = round(etime(datevec(results.daydec(end)),...
%         datevec(results.daydec(1)))/3600)+1;
%     M.daydec = results.daydec(1); %first time is the first measurement
% end


% get the depth from the ground of each layer
depth = flipud(results.thickness);
for n = 1:size(depth,2)
    depth(depth(:,n)==depth(:,n),n) = ...
        cumsum(depth(depth(:,n)==depth(:,n),n),1);
end
depth = flipud(depth);

% first lets evaluate at equal depth intervals for plotting
% at 2cm resolution
xmod = max(results.snowdepth):-0.02:0;
f = fieldnames(results);

% preallocate the structure array
for m = 1:length(f)
    if ~strcmp(f{m},{'daydec';'thickness';'snowdepth';'swe'})
        M.(f{m}) = NaN(length(xmod),telapsed);
    end
end

% m = nan(length(xmod),size(telapsed,2));
f = fieldnames(M);

% ind = find(ismember(results.daydec(tind),M.daydec)); %get days that have data
mind = find(ismember(M.daydec,results.daydec));

for m = 1:length(f)
    if ~strcmp(f{m},'daydec')
        
        for n = 1:length(M.daydec)
            
            % get the values, this will account the time dependent
            % variables like nzz and tf
            nind = ~isnan(results.(f{m})(:,n));
            
            %need at least two layers
            if length(find(nind)) >= 2
                
                try
                    M.(f{m})(:,mind(n)) = interp1(depth(nind,n),...
                        results.(f{m})(nind,n),xmod','linear');
                catch % if error, just fill with NaN for now
                    M.(f{m})(:,mind(n)) = M.(f{m})(:,mind(n));
                end
                
            end
        end
    end
end


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

if nargin > 1
    % plot of the modeled density
    figure
%     figure(2); clf
    subplot(2,2,1)
    imagescnan([min(M.daydec)-7/24 max(M.daydec)-7/24],[min(xmod) max(xmod)],...
        flipud(M.rho),[0 450]);
    colorbar;
    axis([min(M.daydec)-7/24 max(M.daydec)-7/24 min(xmod) max(xmod)+0.05])
    set(gca,'FontSize',14,'FontWeight','bold')
    xlabel('Date');
    datetick('x','mm/dd','keeplimits','keepticks')
    ylabel('estimated layer height [m]')
    set(gca,'YDir','normal');
    title('Modeled density [kg/m^3]')
    set(gcf,'PaperUnits','inches','PaperPosition',[0 0 8 6])
  
    % plot strength
%     figure(3);clf
    subplot(2,2,2)
    imagescnan([min(M.daydec)-7/24 max(M.daydec)-7/24],[min(xmod) max(xmod)],...
        flipud(M.sigfz),[0 max(M.sigfz(:))]);
    colorbar;
    axis([min(M.daydec)-7/24 max(M.daydec)-7/24 min(xmod) max(xmod)+0.05])
    set(gca,'FontSize',14,'FontWeight','bold')
    xlabel('Date');
    datetick('x','mm/dd','keeplimits','keepticks')
    ylabel('estimated layer height [m]')
    set(gca,'YDir','normal');
    title('Strength [Pa]')
    set(gcf,'PaperUnits','inches','PaperPosition',[0 0 8 6])
   
    % plot stability index
%     figure(4);clf
    subplot(2,2,3)
    imagescnan([min(M.daydec)-7/24 max(M.daydec)-7/24],[min(xmod) max(xmod)],...
        flipud(M.stabindex),[params.SIcrit 3*params.SIcrit]);
    colorbar;
    axis([min(M.daydec)-7/24 max(M.daydec)-7/24 min(xmod) max(xmod)+0.05])
    set(gca,'FontSize',14,'FontWeight','bold')
    xlabel('Date');
    datetick('x','mm/dd','keeplimits','keepticks')
    ylabel('estimated layer height [m]')
    set(gca,'YDir','normal');
    title('Stability Index')
    set(gcf,'PaperUnits','inches','PaperPosition',[0 0 8 6])
  
    %plot time to failure
%     figure(5);clf
    subplot(2,2,4)
    imagescnan([min(M.daydec)-7/24 max(M.daydec)-7/24],[min(xmod) max(xmod)],...
        flipud(log10(M.tf)));
    colormap(flipud(colormap))
    colorbar;
    axis([min(M.daydec)-7/24 max(M.daydec)-7/24 min(xmod) max(xmod)+0.05])
    set(gca,'FontSize',14,'FontWeight','bold')
    xlabel('Date');
    datetick('x','mm/dd','keeplimits','keepticks')
    ylabel('estimated layer height [m]')
    set(gca,'YDir','normal');
    title('Time to Failure [log{hr}]')
    set(gcf,'PaperUnits','inches','PaperPosition',[0 0 8 6])
    
    % plot settlement
%     figure(6);clf
%     imagescnan([min(M.daydec)-7/24 max(M.daydec)-7/24],[min(xmod) max(xmod)],...
%         flipud(log10(M.dz)),'NanColor',[1 1 1]);
%     colorbar;
%     axis([min(M.daydec)-7/24 max(M.daydec)-7/24 min(xmod) max(xmod)+0.05])
%     set(gca,'FontSize',14,'FontWeight','bold')
%     xlabel('Date');
%     datetick('x','mm/dd','keeplimits','keepticks')
%     ylabel('estimated layer height [m]')
%     set(gca,'YDir','normal');
%     title('Estimated Settlement [log{mm}]')
%     set(gcf,'PaperUnits','inches','PaperPosition',[0 0 8 6])
%     
end
end