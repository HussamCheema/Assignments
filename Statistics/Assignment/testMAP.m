function [predictedLabels MAPClass0 MAPClass1] = testMAP(X,probVectorClass0,probVectorClass1,prior0,prior1)

TotalSize = size(X,1);
MAPClass1 = [];
MAPClass0 = [];
predictedLabels = [];

% Computation for MAPClass1
for i=1:TotalSize
   
    prob1 = 1;
    
    for j=1:11
    
        if(X(i,j) == 1)
            prob1 = prob1 * probVectorClass1(j);
        else
            prob1 = prob1 * (1-probVectorClass1(j));
        end
    
    end
    
    MAPClass1(i) = log(prob1 * prior1);
    
end

% Computation for MAPClass0
for i=1:TotalSize
   
    prob0 = 1;
    
    for j=1:11
    
        if(X(i,j) == 1)
            prob0 = prob0 * probVectorClass0(j);
        else
            prob0 = prob0 * (1-probVectorClass0(j));
        end
    
    end
    
    MAPClass0(i) = log(prob0 * prior0);
    
end


for k=1:10
    
    if(MAPClass1(k) > MAPClass0(k))
        predictedLabels(k) = 1;
    else
        predictedLabels(k) = 0;
    end
end


end

