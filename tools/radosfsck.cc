/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2014 CERN, Switzerland
 *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License at http://www.gnu.org/licenses/lgpl-3.0.txt
 * for more details.
 */

#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <getopt.h>

#include "Filesystem.hh"
#include "RadosFsChecker.hh"

#define CONF_ENV_VAR "RADOSFS_CLUSTER_CONF"
#define CLUSTER_CONF_ARG "conf"
#define CLUSTER_CONF_ARG_CHAR 'c'
#define CHECK_DIRS_ARG "check-dirs"
#define CHECK_DIRS_CHAR 'd'
#define CHECK_DIRS_RECURSIVE_ARG "recursive"
#define CHECK_DIRS_RECURSIVE_ARG_CHAR 'R'
#define CHECK_INODES_ARG "check-inodes"
#define CHECK_INODES_ARG_CHAR 'i'
#define CHECK_PATHS_ARG "check-paths"
#define CHECK_PATHS_ARG_CHAR 'p'
#define FIX_ARG "fix"
#define FIX_ARG_CHAR 'f'
#define VERBOSE_ARG "verbose"
#define VERBOSE_ARG_CHAR 'v'
#define NUM_THREADS_ARG "threads"
#define NUM_THREADS_ARG_CHAR 't'
#define DEFAULT_NUM_THREADS 4
#define USER_ARG "user"
#define USER_ARG_CHAR 'u'
#define DRY_ARG "dry"
#define DRY_ARG_CHAR 'n'
#define HELP_ARG "help"
#define HELP_ARG_CHAR 'h'
#define OUTPUT_SPAN "%-50s"
#define OPTION_SPAN "    " OUTPUT_SPAN " "
#define SPECIAL_OPTION_SPAN "  * " OUTPUT_SPAN " "

static void
showUsage(const char *name)
{
  fprintf(stdout, "Usage:\n%s ACTIONS [OPTIONS] --%s=CLUSTER_CONF "
          "[POOL_PREFIX:DATA_POOL:MTD_POOL] "
          "[POOL_PREFIX_2:DATA_POOL_2:MTD_POOL_2]\n\n",
          name,
          CLUSTER_CONF_ARG
         );
  fprintf(stdout,
          "  CLUSTER_CONF\t- path to the cluster's configuration file\n"
          "  POOL_PREFIX\t- is the path prefix corresponding to the pools"
          "  DATA_POOL and MTD_POOL\n"
          "  DATA_POOL\t- is the name of the data pool to assign to POOL_PREFIX\n"
          "  MTD_POOL\t- is the name of the metadata pool to assign to "
          "POOL_PREFIX\n"
          "\n"
          "  Optionally more pools and prefixes can also be specified.\n\n"
          "ACTIONS can be:\n"
         );
  std::stringstream arg;
  arg << "--" << CHECK_DIRS_ARG << "=DIR1[,DIR2], -" << CHECK_DIRS_CHAR <<
         " DIR1[,DIR1]";
  fprintf(stdout, SPECIAL_OPTION_SPAN "check the given directories (can be "
                  "together with --%s for checking subdirectories resursively)\n",
          arg.str().c_str(), CHECK_DIRS_RECURSIVE_ARG);

  arg.str("");
  arg << "--" << CHECK_INODES_ARG << "=POOL1[,POOL2], -" << CHECK_DIRS_CHAR <<
         " POOL1[,POOL2]";
  fprintf(stdout, SPECIAL_OPTION_SPAN "check the given  pools' inode objects\n",
          arg.str().c_str());

  arg.str("");
  arg << "--" << CHECK_INODES_ARG << ", -" << CHECK_DIRS_CHAR;
  fprintf(stdout, OPTION_SPAN "check all inode objects in the pools "
                  "specified for the prefixes\n", arg.str().c_str());

  arg.str("");
  arg << "--" << CHECK_PATHS_ARG << "=PATH1[,PATH2], -" <<
         CHECK_PATHS_ARG_CHAR << " PATH1[,PATH2]";
  fprintf(stdout, SPECIAL_OPTION_SPAN "check the given paths only (does not "
                  "check directories' contents)\n", arg.str().c_str());

  fprintf(stdout, "\n  Actions marked with * need to be used together with the "
                  "pools specification given above.\n\n"
                  "OPTIONS can be:\n");

  arg.str("");
  arg << "--" << CHECK_DIRS_RECURSIVE_ARG << ", -" <<
         CHECK_DIRS_RECURSIVE_ARG_CHAR;
  fprintf(stdout, OPTION_SPAN "check the given directories recursively "
                  "(to be used with --%s, otherwise has no effect)\n",
          arg.str().c_str(), CHECK_DIRS_ARG);

  arg.str("");
  arg << "--" << FIX_ARG << ", -" << FIX_ARG_CHAR;
  fprintf(stdout, OPTION_SPAN "fix the issues found (can be used with "
                  "the --%s and --%s actions)\n", arg.str().c_str(),
          CHECK_DIRS_ARG, CHECK_INODES_ARG);

  arg.str("");
  arg << "--" << DRY_ARG << ", -" << DRY_ARG_CHAR;
  fprintf(stdout, OPTION_SPAN "dry run (to be used with the --%s option), "
                  "shows what would be done to fix the issues\n",
          arg.str().c_str(), FIX_ARG);

  arg.str("");
  arg << "--" << NUM_THREADS_ARG << "=NUM_THREADS, -" << NUM_THREADS_ARG_CHAR <<
         " NUM_THREADS";
  fprintf(stdout, OPTION_SPAN "specify number of threads to be used in the "
                  "check (default=%d)\n", arg.str().c_str(),
                  DEFAULT_NUM_THREADS);

  arg.str("");
  arg << "--" << USER_ARG << "=USER_NAME, -" << USER_ARG_CHAR << " USER_NAME";
  fprintf(stdout, OPTION_SPAN "the user name to use when initializing the "
                              "Ceph cluster\n", arg.str().c_str());

  arg.str("");
  arg << "--" << VERBOSE_ARG << ", -" << VERBOSE_ARG_CHAR;
  fprintf(stdout, OPTION_SPAN "display more details about what is being "
                  "done\n", arg.str().c_str());

  arg.str("");
  arg << "--" << HELP_ARG << ", -" << HELP_ARG_CHAR;
  fprintf(stdout, OPTION_SPAN "display help information\n",
          arg.str().c_str());
}

static void
splitToVector(const std::string &str, std::vector<std::string> &vec,
              const char separator = ',')
{
  std::string token;
  for (size_t i = 0; i < str.length(); i++)
  {
    if (str[i] == '\\' && (i + 1) != str.length() && str[i + 1] == separator)
    {
      token += str[++i];
      continue;
    }

    if (str[i] == separator)
    {
      if (!token.empty())
      {
        vec.push_back(token);
        token.clear();
      }

      continue;
    }

    token += str[i];
    continue;
  }

  if (!token.empty())
  {
    vec.push_back(token);
    token.clear();
  }
}

static int
parseArguments(int argc, char **argv,
               std::string &confPath,
               std::string &userName,
               std::vector<std::string> &pools,
               std::vector<std::string> &dirsToCheck,
               bool *checkInodes,
               std::vector<std::string> &poolsToCheckInodes,
               std::vector<std::string> &pathsToCheck,
               int *numThreads,
               bool *recursive,
               bool *fix,
               bool *dry,
               bool *verbose)
{
  confPath = "";
  const char *confFromEnv(getenv(CONF_ENV_VAR));

  if (confFromEnv != 0)
    confPath = confFromEnv;

  int optionIndex = 0;
  struct option options[] =
  {{CLUSTER_CONF_ARG, required_argument, 0, CLUSTER_CONF_ARG_CHAR},
   {CHECK_DIRS_ARG, required_argument, 0, CHECK_DIRS_CHAR},
   {CHECK_DIRS_RECURSIVE_ARG, no_argument, 0, CHECK_DIRS_RECURSIVE_ARG_CHAR},
   {CHECK_INODES_ARG, optional_argument, 0, CHECK_INODES_ARG_CHAR},
   {CHECK_PATHS_ARG, required_argument, 0, CHECK_PATHS_ARG_CHAR},
   {NUM_THREADS_ARG, required_argument, 0, NUM_THREADS_ARG_CHAR},
   {USER_ARG, required_argument, 0, USER_ARG_CHAR},
   {FIX_ARG, no_argument, 0, FIX_ARG_CHAR},
   {DRY_ARG, no_argument, 0, DRY_ARG_CHAR},
   {VERBOSE_ARG, no_argument, 0, VERBOSE_ARG_CHAR},
   {HELP_ARG, no_argument, 0, HELP_ARG_CHAR},
   {0, 0, 0, 0}
  };

  *recursive = false;
  *fix = false;
  *dry = false;
  *verbose = false;
  *numThreads = DEFAULT_NUM_THREADS;
  userName = "";

  std::string args;

  for (int i = 0; options[i].name != 0; i++)
  {
    args += options[i].val;

    if (options[i].has_arg == required_argument)
      args += ":";
    if (options[i].has_arg == optional_argument)
      args += "::";
  }

  int c;
  while ((c = getopt_long(argc, argv, args.c_str(), options, &optionIndex)) != -1)
  {
    switch(c)
    {
      case CLUSTER_CONF_ARG_CHAR:
        confPath = optarg;
        break;
      case CHECK_DIRS_CHAR:
        splitToVector(optarg, dirsToCheck);
        break;
      case CHECK_INODES_ARG_CHAR:
        *checkInodes = true;
        if (optarg)
          splitToVector(optarg, poolsToCheckInodes);

        break;
      case CHECK_PATHS_ARG_CHAR:
        splitToVector(optarg, pathsToCheck);
        break;
      case CHECK_DIRS_RECURSIVE_ARG_CHAR:
        *recursive = true;
        break;
      case DRY_ARG_CHAR:
        *dry = true;
        break;
      case FIX_ARG_CHAR:
        *fix = true;
        break;
      case USER_ARG_CHAR:
        userName = optarg;
        break;
      case NUM_THREADS_ARG_CHAR:
        *numThreads = atoi(optarg);
        if (*numThreads <= 0)
        {
          fprintf(stderr, "Error: The number of threads requested (%d) seems "
                          "to be a mistake, please verify that you have chosen"
                          "a number > 0!", *numThreads);
          return -EINVAL;
        }
        break;
      case VERBOSE_ARG_CHAR:
        *verbose = true;
        break;
      case HELP_ARG_CHAR:
        showUsage(argv[0]);
      default:
        return -1;
    }
  }

  if (confPath == "")
  {
    fprintf(stdout, "Error: Please specify the " CONF_ENV_VAR " environment "
            "variable or use the --" CLUSTER_CONF_ARG "=... argument.\n\n");

    showUsage(argv[0]);

    return -1;
  }

  for (int posArg = optind; posArg < argc; posArg++)
  {
    pools.push_back(argv[posArg]);
  }

  return 0;
}

void
addPools(radosfs::Filesystem &fs, std::vector<std::string> poolsArg)
{
  if (poolsArg.size() == 0)
    return;

  for (size_t i = 0; i < poolsArg.size(); i++)
  {
    std::vector<std::string> poolsForPrefix;
    splitToVector(poolsArg[i], poolsForPrefix, ':');

    if (poolsForPrefix.size() != 3)
    {
      fprintf(stderr, "Invalid argument for describing a prefix and pool: "
                      "'%s'", poolsArg[i].c_str());
      exit(EINVAL);
    }

    const std::string &prefix = poolsForPrefix[0];
    const std::string &dataPool = poolsForPrefix[1];
    const std::string &mtdPool = poolsForPrefix[2];

    int ret;
    if ((ret = fs.addDataPool(dataPool, prefix)) != 0)
    {
      fprintf(stderr, "Problem adding data pool '%s': %s (retcode=%d)",
              dataPool.c_str(), strerror(abs(ret)), ret);
      exit(abs(ret));
    }
    if ((ret = fs.addMetadataPool(mtdPool, prefix)) != 0)
    {
      fprintf(stderr, "Problem adding metadata pool '%s': %s (retcode=%d)",
              mtdPool.c_str(), strerror(abs(ret)), ret);
      exit(abs(ret));
    }
  }
}

int
main(int argc, char **argv)
{
  int ret;
  bool checkInodes, recursive, fix, dry, verbose;
  int numThreads;
  std::string confPath, userName;
  std::vector<std::string> dirsToCheck, poolsToCheckInodes, pools, pathsToCheck;

  ret = parseArguments(argc, argv,
                       confPath,
                       userName,
                       pools,
                       dirsToCheck,
                       &checkInodes,
                       poolsToCheckInodes,
                       pathsToCheck,
                       &numThreads,
                       &recursive,
                       &fix,
                       &dry,
                       &verbose);

  if (ret != 0)
    return ret;

  radosfs::Filesystem radosFs;
  radosFs.init(userName.c_str(), confPath.c_str());

  addPools(radosFs, pools);

  RadosFsChecker checker(&radosFs, numThreads);

  checker.setVerbose(verbose);
  checker.setFix(fix);
  checker.setDry(dry);
  checker.setHasPools(!pools.empty());

  DiagnosticSP diagnostic(new Diagnostic);

  // Verify the user asked for something to be checked
  if (dirsToCheck.empty() && pathsToCheck.empty() &&
      poolsToCheckInodes.empty() && !checkInodes)
  {
    fprintf(stderr, "Please specify one of the following actions:\n"
                    "\t--%s\n"
                    "\t--%s\n"
                    "\t--%s\n\n",
                    CHECK_DIRS_ARG, CHECK_INODES_ARG, CHECK_PATHS_ARG);

    showUsage(argv[0]);

    exit(EOPNOTSUPP);
  }

  // Warn that the fix is being incorrectly used
  if (fix && dirsToCheck.empty() && poolsToCheckInodes.empty() && !checkInodes)
  {
    fprintf(stderr, "The --%s option can only be used with the following "
                    "actions:\n"
                    "\t--%s\n"
                    "\t--%s\n",
                     FIX_ARG, CHECK_DIRS_ARG, CHECK_INODES_ARG);

    exit(EOPNOTSUPP);
  }

  if (dry && !fix)
  {
    fprintf(stderr, "The --%s option can only be used together with the --%s "
                    "option\n", DRY_ARG, FIX_ARG);

    exit(EOPNOTSUPP);
  }

  if (pools.empty() && dirsToCheck.size())
  {
    fprintf(stderr, "No pools and prefixes were configured. This is needed "
                    "in order to check the directories.");
    exit(EINVAL);
  }
  else
  {
    for (size_t i = 0; i < dirsToCheck.size(); i++)
    {
      const std::string &dir(dirsToCheck[i]);

      if (dir[0] != '/')
      {
        fprintf(stderr, "Cannot check '%s'. Please use an absolute path.",
                dir.c_str());
        exit(EINVAL);
      }

      checker.checkDirInThread(dir, recursive, diagnostic);
    }
  }

  if (checkInodes)
  {
    if (poolsToCheckInodes.empty())
    {
      if (pools.empty())
      {
        fprintf(stderr, "No argument is provided to --%s and no pools "
                        "configured. Either one of these options needs to be "
                        "set in order for the inodes to be checked.",
                CHECK_INODES_ARG);
        exit(EINVAL);
      }
      checker.checkInodes(diagnostic);
    }
    else
    {
      std::vector<PoolSP> poolsObjs;
      for (size_t i = 0; i < poolsToCheckInodes.size(); i++)
      {
        const std::string &poolName = poolsToCheckInodes[i];
        PoolSP pool = checker.getPool(poolName);

        if (!pool)
        {
          fprintf(stderr, "Cannot get pool '%s'. Please check if the name is "
                          "correct.\n", poolName.c_str());
          exit(EINVAL);
        }

        poolsObjs.push_back(pool);
      }

      for (size_t i = 0; i < poolsObjs.size(); i++)
      {
        checker.checkInodesInThread(poolsObjs[i], diagnostic);
      }
    }
  }

  if (pools.empty() && pathsToCheck.size())
  {
    fprintf(stderr, "No pools and prefixes were configured. This is needed "
                    "in order to check the paths.");
    exit(EINVAL);
  }
  else
  {
    for (size_t i = 0; i < pathsToCheck.size(); i++)
    {
      const std::string &path(pathsToCheck[i]);

      if (path[0] != '/')
      {
        fprintf(stderr, "Cannot check '%s'. Please use an absolute path.",
                path.c_str());
        exit(EINVAL);
      }

      checker.checkPathInThread(path, diagnostic);
    }
  }

  checker.finishCheck();

  diagnostic->print(checker.errorsDescription, dry);

  return 0;
}
