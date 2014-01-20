
#ifdef __cplusplus
extern "C" 
{
#endif

#if WIN32
typedef unsigned __int64 ub8;
#else
typedef  unsigned long  long ub8;   /* unsigned 8-byte quantities */
#endif

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;



extern ub8 hash64(ub1 * k, ub8 length, ub8 level);
extern ub8 hashParam64(ub1 * k, ub8 length, ub8 moreData, ub8 level);

#ifdef __cplusplus
}  /* end extern "C" */
#endif
