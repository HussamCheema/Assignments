%% Submitted By Hussam-Ul-Hussain, Roll # 18L-1827
% Comment out the sections

clc; clear; close all;

trainX = load('ocr3_7train.txt');
trainY = load('ocr3_7trainLabels.txt');

%% For Number 3 - corresponding to 4 highest eigen values
%%----------------------------------------------------------
for i = 1:4
    ind3 = find(trainY==3);
    X3 = trainX(ind3,:);

    sigma = cov(X3);
    [Vectors,Values] = eig(sigma);

    [Values,I] = sort(diag(Values),'descend');
    Vectors = Vectors(:, I);

    image1 = Vectors(:,i);
    image1 = reshape(image1,16,16);
    figure, imagesc(image1), colormap gray ;
end

%% For Number 3 -  - corresponding to 4 lowest eigen values
%%-----------------------------------------------------------
% for i = 1:4
%     ind3 = find(trainY==3);
%     X3 = trainX(ind3,:);
% 
%     sigma = cov(X3);
%     [Vectors,Values] = eig(sigma);
% 
%     [Values,I] = sort(diag(Values),'ascend');
%     Vectors = Vectors(:, I);
% 
%     image1 = Vectors(:,i);
%     image1 = reshape(image1,16,16);
%     figure, imagesc(image1), colormap gray ;
% end
% 
%% For Number 7 - corresponding to 4 highest eigen values
%%-----------------------------------------------------------
% for i = 1:4
% ind7 = find(trainY==7);
% X7 = trainX(ind7,:);
% 
% sigma = cov(X7);
% [Vectors,Values] = eig(sigma);
% 
% [Values,I] = sort(diag(Values),'descend');
% Vectors = Vectors(:, I);
% 
% image2 = Vectors(:,i);
% image2 = reshape(image2,16,16);
% figure, imagesc(image2), colormap gray ;
% end
% 
%% For Number 7 - corresponding to 4 lowest eigen values
%%-----------------------------------------------------------
% for i = 1:4
% ind7 = find(trainY==7);
% X7 = trainX(ind7,:);
% 
% sigma = cov(X7);
% [Vectors,Values] = eig(sigma);
% 
% [Values,I] = sort(diag(Values),'ascend');
% Vectors = Vectors(:, I);
% 
% image2 = Vectors(:,i);
% image2 = reshape(image2,16,16);
% figure, imagesc(image2), colormap gray ;
% end

