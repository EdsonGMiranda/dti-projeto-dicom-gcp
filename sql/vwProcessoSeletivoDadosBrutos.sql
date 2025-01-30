SELECT  [NmEmail],[CdEscola],[NmEscola], [NmProcessoSeletivo],[DtInscricao],[DtCadastro], [NuInscricao], [CdConcurso],[CdCandidato]
  FROM [BI_PROCESSO_SELETIVO].[dbo].[vwProcessoSeletivoDadosBrutos]
  WHERE [NuAnoIngresso] > 2013;

