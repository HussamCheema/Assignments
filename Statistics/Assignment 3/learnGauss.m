function [meanVecClass0,meanVecClass1,covMatrixClass0,covMatrixClass1,prior0,prior1] = learnGauss(trainX,trainLabels)


TotalSize = size(trainLabels,1);

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

%calculating Mean Vector for Class1 and Class0
for col = 1:11
    
    mean1 = 0;
    mean0 = 0;
    
    for row = 1:1500

        if trainLabels(row) == 1
            mean1 = mean1 + trainX(row,col);
        else
            mean0 = mean0 + trainX(row,col);
        end

    end

        mean1 = mean1/1500;
        mean0 = mean0/1500;
        
        meanVecClass1(col) = mean1;
        meanVecClass0(col) = mean0;
        
    
end


% computing x[row] = (X[col]-mean1[i]) for every feature from x1 to x11 for
% Class1 and Class0
% x = X-Xbar

for col = 1:11
    
    i = 1;
    j = 1;
    
    for row = 1:1500
    
        if trainLabels(row) == 1
           
            Matrix1(col,i) = trainX(row,col) - meanVecClass1(col);
            i=i+1;
           
            
        else
            
            Matrix0(col,j) = trainX(row,col) - meanVecClass0(col);
            j = j+1;
            
        end
        
        
    end
    
end

%computing Varience of 11 features

for feature = 1:11
   
    sum1 = 0;
    sum0 = 0;
    
    for values = 1:length(Matrix1)
       
        value = Matrix1(feature,values);
        sum1 = sum1 + value^2;
        
    end
    
    for values = 1:length(Matrix0)
       
        value = Matrix0(feature,values);
        sum0 = sum0 + value^2;
        
    end
    
    Varience1(feature) = sum1/(i-1);
    Varience0(feature) = sum0/(j-1);
end

%Computing covMatrixClass1

Matrix0 = Matrix0'; 
Matrix1 = Matrix1';

covMatrix0 = cov(Matrix0);
covMatrix1 = cov(Matrix1);


%Replacing diagonal values with variences
for rows=1:11
   
    for cols=1:11
    
        if rows == cols
           covMatrix1(rows,cols) = Varience1(rows); 
           covMatrix0(rows,cols) = Varience0(rows);
        end
        
    end
    
end

covMatrixClass0 = covMatrix0;
covMatrixClass1 = covMatrix1;
prior0 = PC0/1500;
prior1 = PC1/1500;

end