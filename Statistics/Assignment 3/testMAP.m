function [predictedLabels,MAPClass0,MAPClass1] = testMAP(testdat,meanVecClass0, meanVecClass1, covMatrixClass0, covMatrixClass1, prior0, prior1)

for i=1:10
   
       % Getting one row in each iteration
       x = testdat(i,:);
       
       % Taking inverse of Covarience Matrices for Class0 & Class1
       covMatrixClass0_Inverse = inv(covMatrixClass0);
       covMatrixClass1_Inverse = inv(covMatrixClass1);
       
       % Calculating Mahalonobis Distance for Class1 and Class0
       mahalonobisDistClass0 = (x-meanVecClass0)*covMatrixClass0_Inverse*(x-meanVecClass0)';
       mahalonobisDistClass1 = (x-meanVecClass1)*covMatrixClass1_Inverse*(x-meanVecClass1)';
       
       
%        PxClass0 = (1/((2*pi)^11/2)*sqrt(det(covMatrixClass0))) * -sqrt(exp(1))* mahalonobisDistClass0; 
%        PxClass1 = (1/((2*pi)^11/2)*sqrt(det(covMatrixClass1))) * -sqrt(exp(1))* mahalonobisDistClass1;

       % Calculating Multivariate Gaussian Distribution
       PxClass0 = (-1/2*(log((2*pi)^11) + log(det(covMatrixClass0))))-(1/2*mahalonobisDistClass0);
       PxClass1 = (-1/2*(log((2*pi)^11) + log(det(covMatrixClass1))))-(1/2*mahalonobisDistClass1);
       
       % Calculating P(x|C=0).P(C=0) and P(x|C=1).P(C=1)
       nominator0 = (PxClass0) * prior0;
       nominator1 = (PxClass1) * prior1;
       
       Px = nominator0 + nominator1;
       
       % Calculating MAP for Class0 and Class1
       MAPClass0(i) = nominator0/Px;
       MAPClass1(i) = nominator1/Px;
       
       % Checking Predicted Labels
       if MAPClass0(i) > MAPClass1(i)
           predictedLabels(i) = 0;
       else
           predictedLabels(i) = 1;
       end
              
end

disp("-----------------------------------------------");

end