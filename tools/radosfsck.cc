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
#define FIX_ARG "fix"
#define FIX_ARG_CHAR 'f'
#define VERBOSE_ARG "verbose"
#define VERBOSE_ARG_CHAR 'v'
#define DRY_ARG "dry"
#define DRY_ARG_CHAR 'n'
#define HELP_ARG "help"
#define HELP_ARG_CHAR 'h'

static void
showUsage(const char *name)
{
  fprintf(stdout, "Usage:\n%s [OPTIONS] --%s=CLUSTER_CONF POOL "
          "POOL_PREFIX DATA_POOL MTD_POOL "
          "[POOL_PREFIX_2 DATA_POOL_2 MTD_POOL_2]\n\n",
          name,
          CLUSTER_CONF_ARG
         );
  fprintf(stdout,
          " CLUSTER_CONF\t- path to the cluster's configuration file\n"
          " POOL_PREFIX\t- is the path prefix corresponding to the pools "
          " DATA_POOL and MTD_POOL\n"
          " DATA_POOL\t- is the name of the data pool to assign to POOL_PREFIX\n"
          " MTD_POOL\t- is the name of the metadata pool to assign to "
          "POOL_PREFIX\n"
          "   more pools and prefixes can also be specified.\n"
          " \nOPTIONS can be:\n"
         );
  fprintf(stdout, "\t--%s, -%c \t fix the issues found\n",
          FIX_ARG, FIX_ARG_CHAR);
  fprintf(stdout, "\t--%s, -%c \t dry run (to use with the fix option), shows "
          "what would be done to fix the issues\n",
          DRY_ARG, DRY_ARG_CHAR);
  fprintf(stdout, "\t--%s, -%c \t display more details about the issues\n",
          VERBOSE_ARG, VERBOSE_ARG_CHAR);
  fprintf(stdout, "\t--%s, -%c \t displays help information\n",
          HELP_ARG, HELP_ARG_CHAR);
}

static int
parseArguments(int argc, char **argv,
               std::string &confPath,
               bool *fix,
               bool *dry,
               bool *verbose,
               int *position)
{
  confPath = "";
  const char *confFromEnv(getenv(CONF_ENV_VAR));

  if (confFromEnv != 0)
    confPath = confFromEnv;

  int optionIndex = 0;
  struct option options[] =
  {{CLUSTER_CONF_ARG, required_argument, 0, CLUSTER_CONF_ARG_CHAR},
   {FIX_ARG, no_argument, 0, FIX_ARG_CHAR},
   {DRY_ARG, no_argument, 0, DRY_ARG_CHAR},
   {VERBOSE_ARG, no_argument, 0, VERBOSE_ARG_CHAR},
   {HELP_ARG, no_argument, 0, HELP_ARG_CHAR},
   {0, 0, 0, 0}
  };

  int c;
  while ((c = getopt_long(argc, argv, "hfnvc:", options, &optionIndex)) != -1)
  {
    switch(c)
    {
      case CLUSTER_CONF_ARG_CHAR:
        confPath = optarg;
        break;
      case DRY_ARG_CHAR:
        *dry = true;
        break;
      case FIX_ARG_CHAR:
        *fix = true;
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

  int numPosArgs = argc - optind;
  if (numPosArgs > 0 && (numPosArgs % 3) == 0)
  {
    *position = optind;

    return 0;
  }

  fprintf(stdout, "Error: Please specify the pool name and prefix pairs...\n\n");

  showUsage(argv[0]);

  return -EINVAL;
}

int
main(int argc, char **argv)
{
  int ret;
  bool fix, dry, verbose;
  int position;
  std::string confPath;

  ret = parseArguments(argc, argv,
                       confPath,
                       &fix,
                       &dry,
                       &verbose,
                       &position);

  if (ret != 0)
    return ret;

  radosfs::Filesystem radosFs;
  radosFs.init("", confPath.c_str());

  int numPosArgs = argc - position;
  int i = position;

  while (i < position + numPosArgs)
  {
    const char *dataPoolName, *mtdPoolName, *pathPrefix;

    pathPrefix = argv[i];
    dataPoolName = argv[i + 1];
    mtdPoolName = argv[i + 2];

    if ((ret = radosFs.addDataPool(dataPoolName, pathPrefix)) != 0)
    {
      fprintf(stdout, "Problem adding pool '%s'\n", dataPoolName);
      showUsage(argv[0]);
      return ret;
    }

    if ((ret = radosFs.addMetadataPool(mtdPoolName, pathPrefix)) != 0)
    {
      fprintf(stdout, "Problem adding pool '%s'\n", mtdPoolName);
      showUsage(argv[0]);
      return ret;
    }

    i += 3;
  }

  RadosFsChecker checker(&radosFs);

  DiagnosticSP diagnostic(new Diagnostic);
  StatSP stat;

  checker.checkDirInThread(stat, "/", true, diagnostic);

  checker.finishCheck();

  diagnostic->print(checker.errorsDescription);

  return 0;
}
