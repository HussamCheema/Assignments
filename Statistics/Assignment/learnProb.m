function [probVecClass0, probVecClass1, prior0, prior1] = learnProb(trainX,trainLabels)

TotalSize = size(trainLabels,1);
    
XC1 = [];
XC0 = [];

%Total Number of Class = 1
PC1 = 0;
%Total Number of Class = 0
PC0 = 0;

%calculating total no of C=1 & C=0
%looping through each row
for i=1:TotalSize      
    if trainLabels(i) == 1 %if C=1?     
        
        %Probability of Class = 1
        PC1 = PC1 + 1;            
    else %if C=0?          
        %Probability of Class = 1
        PC0 = PC0 + 1;
    end
end

%Calculating Probability Vector Class
for col=1:11
    
    xc1 = 0;
    xc0 = 0;
    
    for row=1:TotalSize
        
        if(trainLabels(row) == 1)
            
            if(trainX(row,col) == 1)
                xc1 = xc1 + 1;
            end  
            
        else
            
            if(trainX(row,col) == 1)
                xc0 = xc0 + 1;
            end  
            
        end
        
    end
    
    XC1(col) = xc1;
    XC0(col) = xc0;
    
end

prior0 = PC0/1500;
prior1 = PC1/1500;

probVecClass1 = XC1/PC1;
probVecClass0 = XC0/PC0;


end %end function

