#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>

/*
 * Exercise 1 Implementation CloudSortRam 
 */
void getMemory(char ***data,int rows, int columns){//get memory for 2d char array
        int i;
        char *p = (char *)malloc(rows*columns*sizeof(char));
                (*data) = (char **)malloc(rows*sizeof(char*));
        for (i=0; i<rows; i++)
                (*data)[i] = &(p[i*101]);
};
void keyMemory(char ***data,int rows,int columns){//get memory for 2d char array
     int i;
        char *p = (char *)malloc(rows*columns*sizeof(char));
                (*data) = (char **)malloc(rows*sizeof(char*));
        for (i=0; i<rows; i++)
                (*data)[i] = &(p[i*11]);
};


int main(int argc, char** argv) {
    /*Define variables and arrays*/
    int numprocs, rank, namelen, i=0, j=0,k=0, p_i,pmin, pmax, step, g=0, lines=0, prolines=0, left=0, 
            *hist, *totalHist, *count, *elemCount, *point, *displs, *sendcounts;
    FILE *fp;
    float s, average, procaverage=0, proclines=0, ss, spec;
    char **dataArray, **procArray, **procKeys, **pivots, **totalPivots, **calcBuff, **sortedArray, *ptr, *ptrs, filename[20];
    const int root=0;   
    char processor_name[MPI_MAX_PROCESSOR_NAME];
   /*MPI Init*/
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &namelen);
      
 /* Process 0 open the file calc the lines and store the rows to "data" array of chars*/ 
     if (rank==0){
         printf("*********************************************************************\n"
                   "****************======CloudSortRAM======*****************************\n"
                   "**********************====Algorithm====******************************\n"
                   "*********************************************************************\n");
         printf("Type the name of input file and press enter\n");
         scanf("%s",filename);//read the name of input file
         char ch;
         fp=fopen(filename, "rb");//open file with fp pointer
         do {//Calc the number of tubles
         ch = fgetc(fp);
         if(ch == '\n')
                lines++;
        } while (ch != EOF);
        printf("*****The file consist of %d tuples******\n",lines);//Print the number of tuples
        getMemory(&dataArray,lines,101);//Allocate memory for the number of tuples
        ptr=&(dataArray[0][0]);//Set pointer on the first position of allocated memory
        i=0;
        rewind(fp);//Set pointer back to the start of file
        while(fgetc(fp) != EOF){//Save the tuples in allocated memory
                        fseek(fp, -1, SEEK_CUR);
                        fgets(ptr+(i*101), 101, fp);
                        i++;
                 }
       fclose(fp); //Close file
           ptr=&(dataArray[0][0]);//Set pointer on the first tuble in memory
       printf("Load Tubles in Memory......................."
               ".....................................................OK!!!\n");
       /*Set Pmin, Pmax and s from console*/
       int ver=lines/numprocs;
       int var=0;
       printf("Type the Pmin (> 1 < Pmax < %d) and press enter\n",ver);
       do{
       
       if (var==0){
           scanf("%d",&pmin);
           if (pmin<=1 || pmin > ver){
                printf("False!!! Type the Pmin (> 1 < Pmax < %d) and press enter\n",ver);
               
           }else 
               var=1;
       }
       if (var==1){
           printf("Type the Pmax (> %d < %d) and press enter\n",pmin,ver); 
           scanf("%d",&pmax);
           if (pmax>=pmin && pmax<=ver){
               var=2;
           }else
               printf("False!!! ");
       }
          
       }while(var<2);
       printf("Type the s > 1 and press enter (!!!!type float with . not ,)\n");
       scanf("%f",&spec);
   }
    /*Broadcast from 0 process Pmin,Pmax, s and number of saved tuples*/
    MPI_Bcast(&pmax, 1, MPI_INT, root, MPI_COMM_WORLD);//pmax
    MPI_Bcast(&pmin, 1, MPI_INT, root, MPI_COMM_WORLD);//pmin
    MPI_Bcast(&spec, 1, MPI_INT, root, MPI_COMM_WORLD);//s
    MPI_Bcast(&lines, 1, MPI_INT, root, MPI_COMM_WORLD);//tubles
    p_i=pmin;//Define p_i
     MPI_Barrier(MPI_COMM_WORLD);
    displs = (int*) malloc(numprocs*sizeof(int));
    sendcounts = (int*) malloc(numprocs*sizeof(int));
    point=(int*)malloc(numprocs*sizeof(int));//Allocate array to save the number of tuples   
    prolines = lines/numprocs;	
    left = lines%numprocs;
    
    /*Calc how many tuples will save every process*/
    for (i=0;i<numprocs;i++){
         point[i]=prolines;
         if((i<left) && (left>0)){
             point[i]=point[i]+1;
         }
    }
    for (i=0;i<numprocs;i++){
        if(rank==i){
            prolines=point[i];
        }
    }
     
    /*Calc how many bytes will receive every process */
    for(i = 0; i<numprocs; i++){
        sendcounts[i] = sizeof(char)*(point[i]*101);
        
        } 
    /*Calc the start pointers that process 0 will use for send data to other processes*/
      displs[0]=0;
      for(i = 1; i<numprocs; i++){
         displs[i] = displs[i-1]+sendcounts[i-1]; 
      }

     getMemory(&procArray,prolines,101);//Allocate memory for data that will receive every process
     /*Split the data and send to other processeis*/
     MPI_Scatterv(ptr,sendcounts, displs, MPI_CHAR,procArray[0],prolines*101,MPI_CHAR,0,MPI_COMM_WORLD);
    
     keyMemory(&procKeys,prolines,11);//Allocate memory to copy only the keys from payloads
     MPI_Barrier(MPI_COMM_WORLD);
     /*Free memory from dataArray in process 0*/
     if (rank==0){
          free(&(dataArray[0][0]));
          free(dataArray);
     }
     ptrs=&(procKeys[0][0]);//set pointer to procKeys memory
     for (j=0;j<prolines;j++){//Copy the keys from payloads in every process in new memory
          memcpy(procKeys[j],procArray[j],10);
          ptrs=ptrs+10;
          *ptrs='\0';
          ptrs=ptrs+1;
      }
MPI_Barrier(MPI_COMM_WORLD);
  do{//do while loop to find s <= init s  if fail rerun the program with other pmin pmax and s  
    if (g>0){//double the pivots
        p_i=p_i*2;
    }
          
    if (p_i>pmax){//if p_i > Pmax the algorith has't find <= s and print message rerun with new values
        if (rank==0){
        printf("Algorithm Fail Rerun the program and set new s.....\n");
        }
        MPI_Finalize();
        exit(0);
    }
     step=(int)prolines/p_i;//calc the step that every process will choose keys for make pivots
     keyMemory(&pivots,p_i,11);//Allocate memory for pivots
     j=0;
     
    for (i=0;i<prolines;i=i+step){//calc keys for pivots
        memcpy(pivots[j],procKeys[i],11);
        if(((i==(step*p_i-2)) && (step>1 )) || (j==p_i-1)){
            i=prolines;
        }
        j++;
    }
     
    keyMemory(&totalPivots,p_i*numprocs,11);//Allocate memory for totalPivots    
    MPI_Barrier(MPI_COMM_WORLD);
    /*Save Pivots from all processes to totalPivots array*/
    MPI_Gather(pivots[0], p_i*11, MPI_CHAR,totalPivots[0], p_i*11, MPI_CHAR, 0, MPI_COMM_WORLD);       
    MPI_Barrier(MPI_COMM_WORLD);
  /*Process 0 sort totalPivots Array*/
   if (rank==0){
        
        char temp[11];
           for (i = 0; i < p_i*numprocs; i++) {
                 for (j = 0; j < p_i*numprocs - 1; j++) {
                        if (strcmp(totalPivots[j], totalPivots[j + 1]) > 0) {
                                strcpy(temp, totalPivots[j]);
                                strcpy(totalPivots[j], totalPivots[j + 1]);
                                strcpy(totalPivots[j + 1], temp);
                        }
                  }
            }
       }
   
    MPI_Barrier(MPI_COMM_WORLD);
    free(&(pivots[0][0]));//free memory
    free(pivots);//free memory
    /*Broadcast totalPivots sorted array to all processes*/
    MPI_Bcast(totalPivots[0], p_i*numprocs*11, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    hist=(int *)calloc(p_i*numprocs+1, sizeof(int)); //Allocate memory for histograms
    /*Calc histogram in every process*/
    for (i=0;i<prolines;i++){
          for (j=0;j<p_i*numprocs;j++){
              if (strcmp(procKeys[i], totalPivots[j]) < 0){
                  hist[j]=hist[j]+1;
                  j=p_i*numprocs;
              }
            else if (strcmp(procKeys[i], totalPivots[j]) == 0){
                 hist[j+1]=hist[j+1]+1;
                 j=p_i*numprocs;
            }
            else if (strcmp(procKeys[i], totalPivots[p_i*numprocs-1]) > 0){
                 hist[p_i*numprocs]=hist[p_i*numprocs]+1;
                 j=p_i*numprocs;
            }
        }
    }
  
   totalHist=(int *)calloc(p_i*numprocs+1, sizeof(int));//Allocate memory for final histogram 
   MPI_Barrier(MPI_COMM_WORLD);
   /*Sum all histograms from from processes to final array named totalHist*/
   MPI_Reduce(hist,totalHist,p_i*numprocs+1,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);
   /*Broadcast totalHist array to all processes*/
   MPI_Bcast(totalHist, p_i*numprocs+1, MPI_INT, root, MPI_COMM_WORLD); 
   i=1;
    MPI_Barrier(MPI_COMM_WORLD); 
    /*Calc pointers from histogram to find how many tuples will calc every process and which tuples will calc every process*/
   if(rank==0){
          average=lines/numprocs;
       for(j=0;j<p_i*numprocs+1;j++){
                procaverage=procaverage+totalHist[j];
                if (proclines==0 && procaverage>=average){
                    proclines=procaverage;//save the total tuples that process 0 will calc
                    point[0]=j;//point which tuples will calc
                    procaverage=0;
                }
                else if ((proclines!=0 && procaverage>=average)||( j==p_i*numprocs)){
                    /*Send the number of tuples that other processes will calc */
                    MPI_Send(&procaverage, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
                    point[i]=j;//point which tuples will calc every process
                    i++;
                    procaverage=0;
                    if (i==numprocs){
                        break;
                    }
                }
            }
   }
   else /*Receive other processes the number of tuples that will calc*/
         MPI_Recv(&proclines, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
   //Broadcast average of number of tuples divided with number of processes
   MPI_Bcast(&average,1, MPI_INT, root, MPI_COMM_WORLD); 
   free(hist);//free memory of hist array
   free(totalHist);//free memory of final hist array
    MPI_Barrier(MPI_COMM_WORLD);
    s=proclines/average;//Calc the s in every process
    
    printf("rank %d s=%f\n",rank,s);
    MPI_Barrier(MPI_COMM_WORLD);
    //Calc the max s from all processes
    MPI_Reduce(&s,&ss,1,MPI_FLOAT,MPI_MAX,0,MPI_COMM_WORLD); 
    //Broadcast the Max s
    MPI_Bcast(&ss,1, MPI_FLOAT, root, MPI_COMM_WORLD); 
    MPI_Barrier(MPI_COMM_WORLD);
    
    g++;
   
  }while(ss>spec);//if Max s is <= than s that we defined in console go next else go on do and double the p_i 
    MPI_Barrier(MPI_COMM_WORLD);
    //Broadcast pointers for know where tuples have to calc every process
    MPI_Bcast(point,numprocs, MPI_INT, root, MPI_COMM_WORLD);
    //Define 3d char array to for put every process the tubles for other processes
    char index[numprocs][prolines+1][101];
    MPI_Barrier(MPI_COMM_WORLD);
    //Define counter to know how many tuples we have put in 3d for every process
    count= (int*) calloc(numprocs,sizeof(int));
    //Put the tuples for other processes in 3d array and calc how many with counter
    for (i=0;i<prolines;i++){
       for (j=0;j<numprocs;j++){
           if (j<numprocs-1){
       if (strcmp(procKeys[i], totalPivots[point[j]]) < 0){
           strcpy(index[j][count[j]],procArray[i]);
           count[j]++;
           j=numprocs;
       }
           }
        else{
            strcpy(index[numprocs-1][count[j]],procArray[i]);
            count[j]++;
        }
     }
 } 
    //Free memory from data that we no need
   free(&(procKeys[0][0]));
   free(procKeys); 
   free(&(procArray[0][0]));
   free(procArray);
   free(&(totalPivots[0][0]));
   free(totalPivots);
   
    //Make array to save how many tuples will receive every process for calc
   elemCount=(int *)malloc(sizeof(int)*numprocs);
   MPI_Barrier(MPI_COMM_WORLD);
  //calc how many tuples will receive every process from other processes
    for( i = 0; i < numprocs; i++){
        MPI_Gather(&(count[i]), 1, MPI_INT, elemCount, 1, MPI_INT, i, MPI_COMM_WORLD);
  }
   //Sum the total tuples that every process will receive from all other processes
   for(i=0;i<numprocs;i++){
    k=k+elemCount[i];  
  }
   //Allocate memory for every process to save the tuples that will receive from other processes
    getMemory(&calcBuff,k,101);
    //calc the size in bytes that will receive a process from every process
  for(i = 0; i<numprocs; i++){
        elemCount[i] = sizeof(char)*(elemCount[i]*101);
        } 
    //Set the points in memory which a process must know to receive the tuples and save true in memory 
  displs[0]=0;
  for( i = 0; i < numprocs-1; i++){
        displs[i+1] = displs[i] + elemCount[i];
  }
  MPI_Barrier(MPI_COMM_WORLD);
   //Every process receive the tuples from other processes and save in memory
   for( i = 0; i < numprocs; i++){
        MPI_Gatherv(index[i][0],count[i]*101 , MPI_CHAR,calcBuff[0], elemCount, displs, MPI_CHAR, i, MPI_COMM_WORLD);
   }
  free(count);
  MPI_Barrier(MPI_COMM_WORLD);
  //Every process sort the tuples
 char temps[101];
           for (i = 0; i < k; i++) {
                 for (j = 0; j < k - 1; j++) {
                        if (strcmp(calcBuff[j], calcBuff[j + 1]) > 0) {
                                strcpy(temps, calcBuff[j]);
                                strcpy(calcBuff[j], calcBuff[j + 1]);
                                strcpy(calcBuff[j + 1], temps);
                        }
                  }
            }

 MPI_Barrier(MPI_COMM_WORLD);
 //Allocate memory for save all sorted lists from all processes
 getMemory(&sortedArray,lines,101);
 //save in array the size of sorted list in every process
 MPI_Gather(&k, 1, MPI_INT, elemCount, 1, MPI_INT, 0, MPI_COMM_WORLD);
 //calc the size in bytes of sorted lists from every process
 for(i = 0; i<numprocs; i++){
		sendcounts[i] = sizeof(char)*(elemCount[i]*101);
	} 
  free(elemCount);
  //set the pointers where the last array will receive the sorted lists from all processes
  displs[0]=0;
          for(i = 1; i<numprocs; i++){
              displs[i]=displs[i-1]+sendcounts[i-1];
          }
  //Save all sorted lists in the final sorted list        
  MPI_Gatherv(calcBuff[0],k*101 , MPI_CHAR,sortedArray[0], sendcounts, displs, MPI_CHAR, 0, MPI_COMM_WORLD);
  free(&(calcBuff[0][0]));
  free(calcBuff);
  free(displs);
  free(sendcounts);
  //Process 0 print in console the sorted tuples that we read from file
  if (rank==0){
      for(i=0;i<lines;i++)
          printf("%s\n",sortedArray[i]);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  //Process 0 print the s and p_i that algorithm ran
  if (rank==0){
  printf("Perfect!! Agorithm run with p_i=%d\n",p_i);
  printf("s=%f\n",ss);
  }
  MPI_Finalize();
}





