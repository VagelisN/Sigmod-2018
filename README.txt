/****************** Test case 1*******************/
/*
 * Both columns have ones
 * expected results: num_of_rows*num_of_rows
 */

/*for (int i = 0; i < num_of_rows; ++i)
{

  original_array[i][1] = 1;
  original_array[i][0] = 1;
}*/
/****************** Test case 1*******************/


/****************** Test case 2*******************/

/*
 * The first column has odd nubers the second column has even numbers
 * expected results: 0
 */

/*int j=1,k=0;
for (int i = 0; i < num_of_rows; ++i)
{

  original_array[i][1] = j;
  original_array[i][0] = k;
  k += 2;
  j += 2;
}*/
/****************** Test case 2*******************/


/****************** Test case 3*******************/
/*
 * first half of the first column is equal to the second half of the second column
 * expected results : num_of_rows/2 * num_of_rows/2
 */

/*for (int i = 0; i < num_of_rows/2; ++i)
{
  original_array[i][1] = 2;
  original_array[i][0] = 1;
}

for (int i = num_of_rows/2; i < num_of_rows; ++i)
{
  original_array[i][1] = 1;
  original_array[i][0] = 2;
}*/
/****************** Test case 3*******************/
